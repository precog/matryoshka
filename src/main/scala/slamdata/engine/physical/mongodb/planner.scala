package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._

import scalaz.{Free => FreeM, Node => _, _}
import scalaz.task.Task

import Scalaz._

object MongoDbPlanner extends Planner[Workflow] {
  import LogicalPlan._

  import slamdata.engine.analysis.fixplate._

  import agg._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  /**
   * This phase works bottom-up to assemble sequences of object dereferences into
   * the format required by MongoDB -- e.g. "foo.bar.baz".
   *
   * This is necessary because MongoDB does not treat object dereference as a 
   * first-class binary operator, and the resulting irregular structure cannot
   * be easily generated without computing this intermediate.
   *
   * This annotation can also be used to help detect two spans of dereferences
   * separated by non-dereference operations. Such "broken" dereferences cannot
   * be translated into a single pipeline operation and require 3 pipeline 
   * operations (or worse): [dereference, middle op, dereference].
   */
  def FieldPhase[A]: PhaseE[LogicalPlan, PlannerError, A, Option[BsonField]] = lpBoundPhaseE {
    type Output = Option[BsonField]
    
    liftPhaseE(Phase { (attr: LPAttr[A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(LPTerm, Output)]) =>
        node.fold[Output](
          read      = Function.const(None), 
          constant  = Function.const(None),
          join      = (left, right, tpe, rel, lproj, rproj) => None,
          invoke    = (func, args) => 
                      if (func == ObjectProject) {
                        // TODO: Make pattern matching safer (i.e. generate error if pattern not matched):
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan.Constant(Data.Str(fieldName))), _) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) => objAttr \ BsonField.Name(fieldName)

                          case None => BsonField.Name(fieldName)
                        })
                      } else if (func == ArrayProject) {
                        // Mongo treats array derefs the same as object derefs.
                        val (objTerm, objAttrOpt) :: (Term(LogicalPlan.Constant(Data.Int(index))), _) :: Nil = args

                        Some(objAttrOpt match {
                          case Some(objAttr) => objAttr \ BsonField.Name(index.toString)

                          case None => BsonField.Name(index.toString)
                        })
                      } else {
                        None
                      },
          free      = Function.const(None),
          let       = (let, in) => None // ???
        )
      }
    })
  }

  /**
   * This phase builds up expression operations from field attributes.
   *
   * As it works its way up the tree, at some point, it will reach a place where
   * the value cannot be computed as an expression operation. The phase will produce
   * None at these points. Further up the tree from such a position, it may again
   * be possible to build expression operations, so this process will naturally
   * result in spans of expressions alternating with spans of nothing (i.e. None).
   *
   * The "holes" represent positions where a pipeline operation or even a workflow
   * task is required to compute the given expression.
   */
  def ExprPhase: PhaseE[LogicalPlan, PlannerError, Option[BsonField], Option[ExprOp]] = lpBoundPhaseE {
    type Output = PlannerError \/ Option[ExprOp]

    toPhaseE(Phase { (attr: LPAttr[Option[BsonField]]) =>
      scanCata(attr) { (fieldAttr: Option[BsonField], node: LogicalPlan[Output]) =>
        def emit(expr: ExprOp): Output = \/- (Some(expr))

        // Promote a bson field annotation to an expr op:
        def promoteField = \/- (fieldAttr.map(ExprOp.DocField.apply _))

        def nothing = \/- (None)

        def invoke(func: Func, args: List[Output]): Output = {
          def invoke1(f: ExprOp => ExprOp) = {
            val x :: Nil = args

            x.map(_.map(f))
          }
          def invoke2(f: (ExprOp, ExprOp) => ExprOp) = {
            val x :: y :: Nil = args

            (x |@| y)(f)
          }

          func match {
            case `Add`      => invoke2(ExprOp.Add.apply _)
            case `Multiply` => invoke2(ExprOp.Multiply.apply _)
            case `Subtract` => invoke2(ExprOp.Subtract.apply _)
            case `Divide`   => invoke2(ExprOp.Divide.apply _)

            case `Eq`       => invoke2(ExprOp.Eq.apply _)
            case `Neq`      => invoke2(ExprOp.Neq.apply _)
            case `Lt`       => invoke2(ExprOp.Lt.apply _)
            case `Lte`      => invoke2(ExprOp.Lte.apply _)
            case `Gt`       => invoke2(ExprOp.Gt.apply _)
            case `Gte`      => invoke2(ExprOp.Gte.apply _)

            case `Concat`   => invoke2(ExprOp.Concat(_, _, Nil))

            case `Count`    => emit(ExprOp.Count)
            case `Sum`      => invoke1(ExprOp.Sum.apply _)
            case `Avg`      => invoke1(ExprOp.Avg.apply _)
            case `Min`      => invoke1(ExprOp.Min.apply _)
            case `Max`      => invoke1(ExprOp.Max.apply _)

            case `ObjectProject`  => promoteField
            case `ArrayProject`   => promoteField

            case _ => nothing
          }
        }

        node.fold[Output](
          read      = name => emit(ExprOp.DocVar.ROOT()),
          constant  = data => Bson.fromData(data).bimap[PlannerError, Option[ExprOp]](
                        _ => PlannerError.NonRepresentableData(data), 
                        d => Some(ExprOp.Literal(d))
                      ),
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          free      = _ => nothing,
          let       = (_, in) => in
        )
      }
    })
  }

  private type EitherPlannerError[A] = PlannerError \/ A

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e.
   * Mongo query expressions. Selectors are only used for the filtering pipeline op,
   * so it's quite possible we build more stuff than is needed (but it doesn't matter, 
   * unneeded annotations will be ignored by the pipeline phase).
   *
   * Like the expression op phase, this one requires bson field annotations.
   *
   * Most expressions cannot be turned into selector expressions without using the
   * "\$where" operator, which allows embedding JavaScript code. Unfortunately, using
   * this operator turns filtering into a full table scan. We should do a pass over
   * the tree to identify partial boolean expressions which can be turned into selectors,
   * factoring out the leftovers for conversion using $where.
   *
   */
  def SelectorPhase: PhaseE[LogicalPlan, PlannerError, Option[BsonField], Option[Selector]] = lpBoundPhaseE {
    type Input = Option[BsonField]
    type Output = Option[Selector]

    liftPhaseE(Phase { (attr: LPAttr[Input]) =>
      scanPara2(attr) { (fieldAttr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        def emit(sel: Selector): Output = Some(sel)

        def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
 
          def extractValue(t: Term[LogicalPlan]): Option[Bson] =
            t.unFix.fold(
              read      = _ => None,
              constant  = data => Bson.fromData(data).toOption,
              join      = (_, _, _, _, _, _) => None,
              invoke    = (_, _) => None,
              free      = _ => None,
              let       = (_, _) => None
            )

          def extractValues(t: Term[LogicalPlan]): Option[List[Bson]] =
            t.unFix.fold(
              read      = _ => None,
              constant  = _ => None,
              join      = (_, _, _, _, _, _) => None,
              invoke    = (_, args) => args.map(extractValue).sequence,
              free      = _ => None,
              let       = (_, _) => None
            )
           
          /**
           * Attempts to extract a BsonField annotation and a Bson value from
           * an argument list of length two (in any order).
           */
          def extractFieldAndSelector: Option[(BsonField, Bson)] = {
            val (t1, f1, _) :: (t2, f2, _) :: Nil = args
            
            val v1 = extractValue(t1)
            val v2 = extractValue(t2)

            f1.map((_, v2)).orElse(f2.map((_, v1))).flatMap {
              case (field, optSel) => 
                optSel.map((field, _))
            }
          }

          /**
           * All the relational operators require a field as one parameter, and 
           * BSON literal value as the other parameter. So we have to try to
           * extract out both a field annotation and a selector and then verify
           * the selector is actually a BSON literal value before we can 
           * construct the relational operator selector. If this fails for any
           * reason, it just means the given expression cannot be represented
           * using MongoDB's query operators, and must instead be written as
           * Javascript using the "$where" operator.
           */
          def relop(f: Bson => Selector.Condition) =
            for {
              (field, value) <- extractFieldAndSelector
            } yield Selector.Doc(Map(field -> Selector.Expr(f(value))))

          def stringOp(f: String => Selector.Condition) =
            for {
              (field, value) <- extractFieldAndSelector
              str <- value match { case Bson.Text(s) => Some(s); case _ => None }
            } yield (Selector.Doc(Map(field -> Selector.Expr(f(str)))))

          def invoke2Nel(f: (Selector, Selector) => Selector) = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y)(f)
          }

          def regexForLikePattern(pattern: String): String = {
            // TODO: handle '\' escapes in the pattern
            val escape: PartialFunction[Char, String] = {
              case '_'                                => "."
              case '%'                                => ".*"
              case c if ("\\^$.|?*+()[{".contains(c)) => "\\" + c
              case c                                  => c.toString
            }
            "^" + pattern.map(escape).mkString + "$"
          }

          func match {
            case `Eq`       => relop(Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _)

            case `Like`     => stringOp(s => Selector.Regex(regexForLikePattern(s), false, false, false, false))

            case `Between`  => {
              val (_, f1, _) :: (t2, _, _) :: Nil = args
              for {
                f <- f1
                args <- extractValues(t2)
              } yield Selector.And(
                Selector.Doc(f -> Selector.Gte(args(0))),
                Selector.Doc(f -> Selector.Lte(args(1)))
              )
            }

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            // case `Not`      => invoke1(Selector.Not.apply _)

            case _ => None
          }
        }

        node.fold[Output](
          read      = _ => None,
          constant  = _ => None,
          join      = (_, _, _, _, _, _) => None,
          invoke    = invoke(_, _),
          free      = _ => None,
          let       = (_, in) => in._3
        )
      }
    })
  }

  private def getOrElse[A, B](b: B)(a: Option[A]): B \/ A = a.map(\/- apply).getOrElse(-\/ apply b)

  /**
   * In ANSI SQL, ORDER BY (AKA Sort) may operate either on fields derived in 
   * the query selection, or fields in the original data set.
   *
   * WHERE and GROUP BY (AKA Filter / GroupBy) may operate only on fields in the
   * original data set (or inline derivations thereof).
   *
   * HAVING may operate on fields in the original data set or fields in the selection.
   *
   * Meanwhile, in a MongoDB pipeline, operators may only reference data in the 
   * original data set prior to a $project (PipelineOp.Project). All fields not
   * referenced in a $project are deleted from the pipeline.
   *
   * Further, MongoDB does not allow any field transformations in sorts or 
   * groupings.
   *
   * This means that we need to perform a LOT of pre-processing:
   *
   * 1. If WHERE or GROUPBY use inline transformations of original fields, then 
   *    these derivations have to be explicitly materialized as fields in the
   *    selection, and then these new fields used for the filtering / grouping.
   *
   * 2. Move Filter and GroupBy to just after the joins, so they can operate
   *    on the original data. These should be translated into MongoDB pipeline 
   *    operators ($match and $group, respectively).
   *
   * 3. For all original fields, we need to augment the selection with these
   *    original fields (using unique names), so they can survive after the
   *    projection, at which point we can insert a MongoDB Sort ($sort).
   *
   */

  /**
   * A helper function to determine if a plan consists solely of dereference 
   * operations. In MongoDB terminology, such a plan is referred to as a 
   * "field".
   */
  def justDerefs(t: Term[LogicalPlan]): Boolean = {
    t.cata { (fa: LogicalPlan[Boolean]) =>
      fa.fold(
        read      = _ => true,
        constant  = _ => false,
        join      = (_, _, _, _, _, _) => false,
        invoke    = (f, _) => f match {
          case `ObjectProject` => true
          case `ArrayProject` => true
          case _ => false
        },
        free      = _ => false,
        let       = (_, _) => false
      )
    }
  }

  /**
   * The pipeline phase tries to turn expressions and selectors into pipeline 
   * operations.
   *
   */
  def PipelinePhase: PhaseE[LogicalPlan, PlannerError, (Option[Selector], Option[ExprOp]), Option[PipelineBuilder]] = lpBoundPhaseE {
    /*
      Notes on new approach:
      
      1. If this node is annotated with an ExprOp, DON'T DO ANYTHING.
      2. If this node is NOT annotated with an ExprOp, we need to try to create a Pipeline.
          a. If the children have Pipelines, then use those to form the new pipeline in a function-specific fashion.
          b. If the children don't have Pipelines, try to promote them to pipelines in a function-specific manner,
             then use those pipelines to form the new pipeline in a function-specific manner.
          c. If the node cannot be converted to a Pipeline, the process ends here.
  
    */
    type Input  = (Option[Selector], Option[ExprOp])
    type Output = PlannerError \/ Option[PipelineBuilder]

    import PipelineOp._

    def nothing = \/- (None)

    object HasExpr {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[ExprOp] = v.unFix.attr._1._2
    }

    object HasField {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[BsonField] = HasExpr.unapply(v) collect {
        case ExprOp.DocVar(_, Some(f)) => f
      }
    }

    object HasLiteral {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Bson] = HasExpr.unapply(v) collect {
        case ExprOp.Literal(d) => d
      }
    }

    object HasStringConstant {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[String] = HasLiteral.unapply(node).collect { 
        case Bson.Text(str) => str
      }
    }

    object HasSelector {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Selector] = v.unFix.attr._1._1
    }

    object HasPipeline {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[List[PipelineOp]] = {
        val defaultCase = v.unFix.attr._2.toOption.flatten.map(_.buffer)

        v.unFix.unAnn.fold(
          read      = _ => defaultCase orElse (Some(Nil)),
          constant  = _ => defaultCase,
          join      = (_, _, _, _, _, _) => defaultCase,
          invoke    = (_, _) => defaultCase,
          free      = _ => defaultCase,
          let       = (_, _) => defaultCase
        )
      }
    }

    type OpSplit[A <: PipelineOp] = (List[ShapePreservingOp], A, List[PipelineOp])

    type ProjectSplit = OpSplit[Project]

    object HasProject {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[ProjectSplit] = {
        v.unFix.attr._2.toOption.flatten.map(_.buffer).flatMap { ops =>
          val prefix = (ops.takeWhile {
            case x : ShapePreservingOp => true
            case _ => false
          }).collect {
            case x : ShapePreservingOp => x
          }

          val rest = ops.drop(prefix.length)

          val project = rest.headOption.collect {
            case x : Project => x
          }

          val tail = rest.drop(1)

          project.map((prefix, _, tail))
        }
      }
    }

    object IsArray {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[List[Attr[LogicalPlan, (Input, Output)]]] = {
        node.unFix.unAnn match {
          case MakeArray(x :: Nil) =>
            Some(x :: Nil)

          case ArrayConcat(x :: y :: Nil) =>
            (unapply(x) |@| unapply(y))(_ ::: _)

          case _ => None
        }
      }
    }

    object IsObject {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[List[(Attr[LogicalPlan, (Input, Output)], Attr[LogicalPlan, (Input, Output)])]] = {
        node.unFix.unAnn match {
          case MakeObject(x :: y :: Nil) =>
            Some((x, y) :: Nil)

          case ObjectConcat(x :: y :: Nil) =>
            (unapply(x) |@| unapply(y))(_ ::: _)

          case _ => None
        }
      }
    }

    object AllExprs {
      def unapply(args: List[Attr[LogicalPlan, (Input, Output)]]): Option[List[ExprOp]] = args.map(_.unFix.attr._1._2).sequenceU
    }

    object AllFields {
      def unapply(args: List[Attr[LogicalPlan, (Input, Output)]]): Option[List[BsonField]] = args match {
        case AllExprs(exprs) => 
          (exprs.map {
            case ExprOp.DocVar(_, Some(field)) => Some(field)
            case _ => None
          }).sequenceU

        case _ => None
      }
    }

    object IsSortKey {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[(BsonField, SortType)] = 
        node match {
          case IsObject((HasStringConstant("key"), HasField(key)) ::
                            (HasStringConstant("order"), HasStringConstant(orderStr)) :: 
                            Nil)
                  => Some(key -> (if (orderStr == "ASC") Ascending else Descending))
                  
           case _ => None
        }
    }
    
    object AllSortKeys {
      def unapply(args: List[Attr[LogicalPlan, (Input, Output)]]): Option[List[(BsonField, SortType)]] = 
        args.map(IsSortKey.unapply(_)).sequenceU
    }

    object HasSortFields {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[NonEmptyList[(BsonField, SortType)]] = {
        v match {
          case IsArray(AllSortKeys(k :: ks)) => Some(NonEmptyList.nel(k, ks))
          // case IsArray(AllFields(x :: xs)) => Some(NonEmptyList.nel(x, xs).map(_ -> Ascending))
          case _ => None
        }
      }
    }

    def mergeRev(left: List[PipelineOp], right: List[PipelineOp]): Output = {
      mergeRev0(left, MergePatch.Id, right, MergePatch.Id).map(t => Some(PipelineBuilder(t._1)))
    }

    def mergeRev0(left: List[PipelineOp], lp: MergePatch, right: List[PipelineOp], rp: MergePatch): PlannerError \/ (List[PipelineOp], MergePatch, MergePatch) = {
      PipelineOp.mergeOps(Nil, left.reverse, lp, right.reverse, rp).leftMap(e => PlannerError.InternalError(e.message)).map {
        case (ops, lp, rp) => (ops.reverse, lp, rp)
      }
    }

    def emit[A](a: A): PlannerError \/ A = \/- (a)

    def error[A](msg: String): PlannerError \/ A = -\/ (PlannerError.InternalError(msg))

    def projField(ts: (String, ExprOp \/ Reshape)*): Reshape = Reshape.Doc(ts.map(t => BsonField.Name(t._1) -> t._2).toMap)
    def projIndex(ts: (Int,    ExprOp \/ Reshape)*): Reshape = Reshape.Arr(ts.map(t => BsonField.Index(t._1) -> t._2).toMap)

    def emitOps(ops: List[PipelineOp]): Output = \/-(Some(PipelineBuilder(ops)))

    def combine[A <: PipelineOp, B <: PipelineOp](t1: OpSplit[A], t2: OpSplit[B])(f: PartialFunction[(A, B), PlannerError \/ (List[PipelineOp], MergePatch, MergePatch)]): PlannerError \/ (List[PipelineOp], MergePatch, MergePatch) = {

      val (lafter, lop0, lbefore) = t1
      val (rafter, rop0, rbefore) = t2

      for {
        beforet <- mergeRev0(lbefore, MergePatch.Id, rbefore, MergePatch.Id)

        (before, lp0, rp0) = beforet

        (lop, lp1) = lp0(lop0)
        (rop, rp1) = rp0(rop0)

        _ <- if (f.isDefinedAt((lop, rop))) \/-(()) else -\/ (PlannerError.InternalError("Combining " + lop + " and " + rop + " is undefined"))
        
        ft <- f(lop, rop).leftMap(e => PlannerError.InternalError(e.message))

        (mid, lp2, rp2) = ft

        aftert  <- mergeRev0(lafter, lp1 |+| lp2, rafter, rp1 |+| rp2)

        (after, lp3, rp3) = aftert
      } yield (after ::: mid ::: before, lp3, rp3)
    }

    def combineOut[A <: PipelineOp, B <: PipelineOp](t1: OpSplit[A], t2: OpSplit[B])(f: PartialFunction[(A, B), PlannerError \/ (List[PipelineOp], MergePatch, MergePatch)]): Output = {
      combine(t1, t2)(f).map(t => Some(PipelineBuilder(t._1)))
    }

    def reviseHistory[A <: PipelineOp](t: OpSplit[A])(f: PartialFunction[A, PlannerError \/ (List[PipelineOp], MergePatch, MergePatch)]): Output = {
      combineOut[A, A](t, t)(new PartialFunction[(A, A), PlannerError \/ (List[PipelineOp], MergePatch, MergePatch)] {
        def isDefinedAt(t: (A, A)) = f.isDefinedAt(t._1)

        def apply(t: (A, A)) = f(t._1)
      })
    }

    def combineMerge[A <: PipelineOp](t1: (List[ShapePreservingOp], A, List[PipelineOp]), t2: (List[ShapePreservingOp], A, List[PipelineOp])): Output = 
      combineOut(t1, t2) { case (a, b) => a.merge(b).map(_.tuple).leftMap(e => PlannerError.InternalError(e.message)) }

    def splitProjectGroup(r: Reshape, by: ExprOp \/ Reshape): (Project, Group) = {
      r match {
        case Reshape.Doc(v) => 
          val (gs, ps) = ((v.toList.map {
            case (n, -\/(e : ExprOp.GroupOp)) => ((n -> e) :: Nil, Nil)
            case t @ (_,  _) => (Nil, t :: Nil)
            case _ => sys.error("oh no")
          }).unzip : (List[List[(BsonField.Name, ExprOp.GroupOp)]], List[List[(BsonField.Name, ExprOp \/ Reshape)]])).bimap(_.flatten, _.flatten)

          Project(Reshape.Doc(ps.toMap)) -> Group(Grouped(gs.toMap), by)

        case Reshape.Arr(v) =>
          val (gs, ps) = ((v.toList.map {
            case (n, -\/(e : ExprOp.GroupOp)) => ((n -> e) :: Nil, Nil)
            case t @ (_,  _) => (Nil, t :: Nil)
            case _ => sys.error("oh no")
          }).unzip : (List[List[(BsonField.Index, ExprOp.GroupOp)]], List[List[(BsonField.Index, ExprOp \/ Reshape)]])).bimap(_.flatten, _.flatten)

          Project(Reshape.Arr(ps.toMap)) -> Group(Grouped(gs.toMap), by)
      }
    }

    def sortBy(r0: Reshape, by: ExprOp \/ Reshape): (Project, Sort) = {
      val (sortFields, r) = r0 match {
        case Reshape.Doc(m) => 
          val field = BsonField.genUniqName(m.keys)

          NonEmptyList(field -> Ascending) -> Reshape.Doc(m + (field -> by))

        case Reshape.Arr(m) => 
          val field = BsonField.genUniqIndex(m.keys)

          NonEmptyList(field -> Ascending) -> Reshape.Arr(m + (field -> by))
      }

      Project(r) -> Sort(sortFields)
    }

    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Output)]]): Output = {
      func match {
        case `MakeArray` => 
          args match {
            // TODO: Allow shape preserving ops after project
            case HasPipeline(Project(reshape) :: tail) :: Nil => 
              emitOps(Project(reshape.nestIndex(0)) :: tail)

            case HasExpr(e) :: Nil => emitOps(Project(projIndex(0 -> -\/(e))) :: Nil)
            
            case _ => error("Cannot compile a MakeArray because neither an expression nor a reshape pipeline were found")
          }

        case `MakeObject` =>
          args match {
            // TODO: Allow shape preserving ops after project
            case HasLiteral(Bson.Text(name)) :: HasPipeline(Project(reshape) :: tail) :: Nil => 
              emitOps(Project(reshape.nestField(name)) :: tail)

            case HasLiteral(Bson.Text(name)) :: HasExpr(e) :: Nil => emitOps(Project(projField(name -> -\/(e))) :: Nil)

            case _ => error("Cannot compile a MakeObject because neither an expression nor a reshape pipeline were found")
          }
        
        case `ObjectConcat` =>
          args match {
            case HasProject(t1) :: HasProject(t2) :: Nil => combineMerge(t1, t2)

            case _ => error("Cannot compile an ObjectConcat because both sides are not projects")
          }
        
        case `ArrayConcat` =>
          args match {
            case HasProject(t1) :: HasProject(t2) :: Nil => 
              combineOut(t1, t2) {
                case (Project(l), Project(r)) => 
                  val (m, rp) = l.merge(r, ExprOp.DocVar.ROOT())

                  \/- ((Project(m) :: Nil, MergePatch.Id, rp))
              }

            case _ => error("Cannot compile an ArrayConcat because both sides are not projects building arrays")
          }

        case `Filter` => 
          args match {
            case HasPipeline(ops) :: HasSelector(q) :: Nil => emitOps(Match(q) :: ops)

            case _ => error("Cannot compile a Filter because the set has no pipeline or the predicate has no selector: " + args(1))
          }

        case `Drop` =>
          args match {
            case HasPipeline(ops) :: HasLiteral(Bson.Int64(v)) :: Nil => emitOps(Skip(v) :: ops)

            case _ => error("Cannot compile Drop because the set has no pipeline or number has no literal")
          }
        
        case `Take` => 
          args match {
            case HasPipeline(ops) :: HasLiteral(Bson.Int64(v)) :: Nil => emitOps(Limit(v) :: ops)

            case _ => error("Cannot compile Take because the set has no pipeline or number has no literal")
          }

        case `GroupBy` =>
          args match {
            case HasProject(t1) :: HasProject(t2) :: Nil => 
              combineOut(t1, t2) {
                case (Project(r1 @ Reshape(_)), Project(r2 @ Reshape(_))) =>
                  val (p, g) = splitProjectGroup(r1, \/-(r2))

                  val r1Doc = r1.toDoc

                  val names = r1Doc.value.keys

                  val groupByName = BsonField.genUniqName(names)

                  val rightPatch = MergePatch.Rename(ExprOp.DocVar.ROOT(), ExprOp.DocVar.ROOT(groupByName))

                  val ops = Project(Reshape.Doc(r1Doc.value + (groupByName -> \/-(r2)))) :: g :: Nil

                  \/- ((ops, MergePatch.Id, rightPatch))
              }

            case HasProject(post, Project(r), pre) :: HasExpr(e) :: Nil => 
              // TODO: Flatten out nesting!!!!!!!!!!!!!!!!
              val (p, g) = splitProjectGroup(r, -\/(e))

              combineMerge((post, p, pre), (post, g, pre))

            case _ => error("Cannot compile GroupBy because a projection or a projection / expression could not be extracted")
          }

        case `OrderBy` =>
// { for {t <- args} println("arg: " + t)
          args match {
            case HasProject(post, Project(r0), pre) :: HasExpr(e) :: Nil =>
              val (proj, sort) = sortBy(r0, -\/(e))

              emitOps(sort :: post ::: proj :: pre)

            case HasProject(t1) :: HasProject(t2) :: Nil =>
              combineOut(t1, t2) {
                case (Project(r1), Project(r2)) =>
                  val (proj, sort) = sortBy(r1, \/-(r2))

                  val ops = sort :: proj :: Nil

                  \/- ((ops, MergePatch.Id, MergePatch.Id))
              }

            case HasPipeline(ops) :: HasSortFields(fields) :: Nil =>
              // TODO: Can generalize this simply by adding patches to PipelineBuilder so we can propagate
              // rename / nest information through the whole tree.
              emitOps(Sort(fields) :: ops) 

            case _ => error("Cannot compile OrderBy because cannot extract out a project and a project / expression: " + args)
          }
// }
        case `ObjectProject` =>
          args match {
            case HasPipeline((proj @ Project(_)) :: tail) :: HasLiteral(Bson.Text(field)) :: Nil =>
              proj.field(field).map { _ fold(
                  _ => nothing, // FIXME!!! This indicates a deref resulted in an expression which cannot be translated into pipeline op.
                  projField => emitOps(projField :: tail)
                )
              } getOrElse (error("No field of value " + field + " could be found"))

            case HasProject(t) :: HasLiteral(Bson.Text(field)) :: Nil =>
              reviseHistory(t) {
                case proj @ Project(_) =>
                  val patch = MergePatch.Rename(ExprOp.DocVar.ROOT(BsonField.Name(field)), ExprOp.DocVar.ROOT())

                  proj.field(field).map { 
                    _ fold(
                      _         => error("Cannot project into expression"),
                      projField => \/- (projField :: Nil, patch, patch)
                    )
                  }.getOrElse(error("Could not find the field " + field))
              }

            case _ => error("Unknown format for object project: " + args.mkString("\n"))
          }
        
        case `ArrayProject` =>
          args match {
            // TODO: Generalize this so we can handle operations after the Project.
            case HasPipeline((proj @ Project(_)) :: tail) :: HasLiteral(Bson.Int64(idx)) :: Nil =>
              proj.index(idx.toInt).map { _ fold(
                  _ => nothing, // FIXME!!! This indicates a deref resulted in an expression which cannot be translated into pipeline op.
                  projField => emitOps(projField :: tail)
                )
              } getOrElse (error("No index of value " + idx + " could be found"))

            case _ => error("Unknown format for array project: " + args.mkString("\n"))
          }
        
        case _ => error("Function " + func + " cannot be compiled to a pipeline op")
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      println(Show[Attr[LogicalPlan, Input]].show(attr).toString)

      val attr2 = scanPara0(attr) { (orig: Attr[LogicalPlan, Input], node: LogicalPlan[Attr[LogicalPlan, (Input, Output)]]) =>
        val (optSel, optExprOp) = orig.unFix.attr

        // Only try to build pipelines on nodes not annotated with an expr op, but
        // propagate pipelines on other expression types:
        optExprOp.map { _ =>
          node.fold[Output](
            read      = _ => nothing,
            constant  = _ => nothing,
            join      = (_, _, _, _, _, _) => nothing,
            invoke    = (f, vs) => {
                          val ps: List[Option[Pipeline]] = vs.map(_.unFix.attr).map {
                            case ((_, _), \/-(Some(pOp))) => Some(pOp.build)
                            case _ => None
                          }
                          
                          // If at least one argument has a pipeline...
                          if (ps.exists(_.isDefined)) {
                            // We have to create a pipeline for this node:
                            invoke(f, vs)
                          } else nothing
                        },
            free      = _ => nothing,
            let       = (let, in) => in.unFix.attr._2
          )
        } getOrElse {
          node.fold[Output](
            read      = _ => nothing,
            constant  = _ => nothing,
            join      = (_, _, _, _, _, _) => nothing,
            invoke    = invoke(_, _),
            free      = _ => nothing,
            let       = (let, in) => in.unFix.attr._2
          )
        }
      }

      // println(Show[Attr[LogicalPlan, Output]].show(attr2).toString)

      attr2
    })
  }

  private def collectReads(t: Term[LogicalPlan]): List[Path] = {
    t.foldMap[List[Path]] { term =>
      term.unFix.fold(
        read      = _ :: Nil,
        constant  = _ => Nil,
        join      = (_, _, _, _, _, _) => Nil,
        invoke    = (_, _) => Nil,
        free      = _ => Nil,
        let       = (_, _) => Nil
      )
    }
  }

  val AllPhases = (FieldPhase[Unit]).fork(SelectorPhase, ExprPhase) >>> PipelinePhase

  def plan(logical: Term[LogicalPlan]): PlannerError \/ Workflow = {
    import WorkflowTask._

    def trivial(p: Path) = \/- (Workflow(WorkflowTask.ReadTask(Collection(p.filename))))

    def nonTrivial = {
      val paths = collectReads(logical)

      AllPhases(attrUnit(logical)).map(_.unFix.attr).flatMap { pbOpt =>
        paths match {
          case path :: Nil => 
            val read = WorkflowTask.ReadTask(Collection(path.filename))

            pbOpt match {
              case Some(builder) => \/- (Workflow(WorkflowTask.PipelineTask(read, builder.build)))

              case None => -\/ (PlannerError.InternalError("The plan cannot yet be compiled to a MongoDB workflow"))
            }

          case _ => -\/ (PlannerError.InternalError("Pipeline compiler requires a single source for reading data from"))
        }
      }
    }

    logical.unFix.fold(
      read      = p => trivial(p),
      constant  = _ => nonTrivial,
      join      = (_, _, _, _, _, _) => nonTrivial,
      invoke    = (_, _) => nonTrivial,
      free      = _ => nonTrivial,
      let       = (_, _) => nonTrivial
    )
  }
}
