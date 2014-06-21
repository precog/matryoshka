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

  import set._
  import relations._
  import structural._
  import math._
  import agg._

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
          /**
           * Attempts to extract a BsonField annotation and a selector from
           * an argument list of length two (in any order).
           */
          def extractFieldAndSelector: Option[(BsonField, Selector)] = {
            val (_, f1, s1) :: (_, f2, s2) :: Nil = args

            f1.map((_, s2)).orElse(f2.map((_, s1))).flatMap {
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
          def relop(f: Bson => Selector) = {
            extractFieldAndSelector.flatMap[Selector] {
              case (field, selector) => 
                selector match {
                  case Selector.Literal(bson) => Some(Selector.Doc(Map(field -> f(bson))))
                  case _ => None
                }
            }
          }
          def invoke1(f: Selector => Selector) = {
            val x :: Nil = args.map(_._3)

            x.map(f)
          }
          def invoke2Nel(f: NonEmptyList[Selector] => Selector) = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y)((a, b) => f(NonEmptyList(a, b)))
          }

          func match {
            case `Eq`       => relop(Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _)

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            case `Not`      => invoke1(Selector.Not.apply _)

            case _ => None
          }
        }

        node.fold[Output](
          read      = _ => None,
          constant  = data => Bson.fromData(data).fold[Output](
                        _ => None, 
                        d => Some(Selector.Literal(d))
                      ),
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

    object HasLiteral {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Bson] = HasExpr.unapply(v) collect {
        case ExprOp.Literal(d) => d
      }
    }

    object HasQuerySpec {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Selector] = v.unFix.attr._1._1
    }

    object HasPipeline {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[List[PipelineOp]] = v.unFix.attr._2.toOption.flatten.map(_.buffer)
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

    def mergeRev(ops1: List[PipelineOp], ops2: List[PipelineOp]): Output = {
      Pipeline(ops1.reverse).merge(Pipeline(ops2.reverse)).map(p => Some(PipelineBuilder(p))).leftMap(e => PlannerError.InternalError(e.message))
    }

    def mergeRev0(left: List[PipelineOp], lp: MergePatch, right: List[PipelineOp], rp: MergePatch): 
        PlannerError \/ (List[PipelineOp], MergePatch, MergePatch) = {
      PipelineOp.mergeOps(Nil, left.reverse, lp, right.reverse, rp).leftMap(e => PlannerError.InternalError(e.message))
    }

    def emit[A](a: A): PlannerError \/ A = \/- (a)

    def error[A](msg: String): PlannerError \/ A = -\/ (PlannerError.InternalError(msg))

    def projField(ts: (String, ExprOp \/ Reshape)*): Reshape = Reshape.Doc(ts.map(t => BsonField.Name(t._1) -> t._2).toMap)
    def projIndex(ts: (Int,    ExprOp \/ Reshape)*): Reshape = Reshape.Arr(ts.map(t => BsonField.Index(t._1) -> t._2).toMap)

    def emitOps(ops: List[PipelineOp]): Output = \/-(Some(PipelineBuilder(ops)))



    def combine[A <: PipelineOp, B <: PipelineOp](t1: OpSplit[A], t2: OpSplit[B])
        (f: PartialFunction[(A, B), PipelineOpMergeError \/ (List[PipelineOp], MergePatch, MergePatch)]): 
          PlannerError \/ (List[PipelineOp], MergePatch, MergePatch) = {

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

    def combineOut[A <: PipelineOp, B <: PipelineOp](t1: OpSplit[A], t2: OpSplit[B])
        (f: PartialFunction[(A, B), PipelineOpMergeError \/ (List[PipelineOp], MergePatch, MergePatch)]): Output = {
      combine(t1, t2)(f).map(t => Some(PipelineBuilder(t._1)))
    }

    def combineMerge[A <: PipelineOp](t1: (List[ShapePreservingOp], A, List[PipelineOp]), t2: (List[ShapePreservingOp], A, List[PipelineOp])): Output = 
      combineOut(t1, t2) { case (a, b) => a.merge(b).map(_.tuple) }

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
            case HasPipeline(Project(reshape) :: tail) :: Nil => emitOps(Project(reshape.nestIndex(0)) :: tail)

            case HasExpr(e) :: Nil => emitOps(Project(projIndex(0 -> -\/(e))) :: Nil)
            
            case _ => error("Cannot compile a MakeArray because neither an expression nor a reshape pipeline were found")
          }

        case `MakeObject` =>
          args match {
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
            case HasProject(t1 @ (_, Project(Reshape.Arr(_)), _)) :: 
                 HasProject(t2 @ (_, Project(Reshape.Arr(_)), _)) :: Nil => 

              combineOut(t1, t2) {
                case (Project(r1 @ Reshape.Arr(_)), Project(r2 @ Reshape.Arr(_))) =>
                  val rightOffset = r1.maxIndex.map(_ + 1).getOrElse(0)

                  \/- ((Project(Reshape.Arr(r1.value ++ r2.offset(rightOffset).value)) :: Nil, MergePatch.Id, MergePatch.Id))
              }

            case _ => error("Cannot compile an ArrayConcat because both sides are not projects building arrays")
          }

        case `Filter` => 
          args match {
            case HasPipeline(ops) :: HasQuerySpec(q) :: Nil => emitOps(Match(q) :: ops)

            case _ => error("Cannot compile a Filter because the set has no pipeline or the predicate has no selector")
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

            case _ => error("Cannot compile GroupBy")
          }

        case `OrderBy` =>
          args match {
            case HasProject(post, Project(r0), pre) :: HasExpr(e) :: Nil =>
              val (proj, sort) = sortBy(r0, -\/(e))

              emitOps(sort :: post ::: proj :: pre)

            case HasProject(t1) :: HasProject(t2) :: Nil =>
              combineOut(t1, t2) {
                case (Project(r1), Project(r2)) =>
                  val (proj, sort) = sortBy(r1, \/-(r2))

                  val ops = proj :: sort :: Nil

                  \/- ((ops, MergePatch.Id, MergePatch.Id))
              }
          }
        
        case _ => nothing
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)
      scanPara0(attr) { (orig: Attr[LogicalPlan, Input], node: LogicalPlan[Attr[LogicalPlan, (Input, Output)]]) =>
        val (optSel, optExprOp) = orig.unFix.attr

        // Only try to build pipelines on nodes not annotated with an expr op:
        optExprOp.map(_ => nothing) getOrElse {
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
    })

    /*

    def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
      def merge(left: List[PipelineOp], right: List[PipelineOp]): PlannerError \/ List[PipelineOp] = 
        if (left != right) -\/ (PlannerError.InternalError("Diverging pipeline histories: " + left + ", " + right))
        else \/- (left)

      def selector(v: (Term[LogicalPlan], Input, Output)): Option[Selector] = (v _2) _1

      def exprOp(v: (Term[LogicalPlan], Input, Output)): Option[ExprOp] = (v _2) _2

      def pipelineOp(v: (Term[LogicalPlan], Input, Output)): Option[List[PipelineOp]] = (v _3).toOption.flatten

      def constant(v: (Term[LogicalPlan], Input, Output)): Option[Data] = v._1.unFix.fold(
        read      = _ => None,
        constant  = Some(_),
        join      = (_, _, _, _, _, _) => None,
        invoke    = (_, _) => None,
        free      = _ => None,
        let       = (_, _) => None
      )

      def constantStr(v: (Term[LogicalPlan], Input, Output)): Option[String] = constant(v) collect {
        case Data.Str(text) => text
      }

      def constantLong(v: (Term[LogicalPlan], Input, Output)): Option[Long] = constant(v) collect {
        case Data.Int(v) => v.toLong
      }

      def getOrFail[A](msg: String)(a: Option[A]): PlannerError \/ A = a.map(\/-.apply).getOrElse(-\/(PlannerError.InternalError(msg + ": " + args)))

      func match {
        case `MakeArray` => 
          getOrFail("Expected to find an expression for array argument")(args match {
            case value :: Nil =>
              for {
                // FIXME: pipelineOps on value
                value <- exprOp(value)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map(BsonField.Name("0") -> -\/(value)))) :: Nil

            case _ => None
          })
          
        case `ObjectProject` => 
          getOrFail("Expected nothing in particluar for projection")(args match {
            case obj :: field :: Nil => pipelineOp(obj) // Propagate pipeline ops attached to "obj"
            case _ => None
          })

        case `MakeObject` =>
          getOrFail("Expected to find string for field name and expression for field value")(args match {
            case field :: obj :: Nil =>
              for {
                ops   <- pipelineOp(obj) // FIXME
                field <- constantStr(field)
                obj   <- exprOp(obj)
              } yield PipelineOp.Project(PipelineOp.Reshape(Map(BsonField.Name(field) -> -\/(obj)))) :: ops

            case _ => None
          })
        
        case `ObjectConcat` => 
          getOrFail("Expected both left and right of object concat to be projection pipeline ops")(args match {
            case left :: right :: Nil =>
              for {
                left      <- pipelineOp(left)
                right     <- pipelineOp(right)
                leftMap   <- left.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
                rightMap  <- right.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
                
                ltail = left.tailOption.getOrElse(Nil)
                rtail = right.tailOption.getOrElse(Nil)

                tail      <- merge(ltail, rtail).toOption // FIXME
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: tail

            case _ => None
          })
        
        case `ArrayConcat` => 
          getOrFail("Expected both left and right of array concat to be projection pipeline ops")(args match {
            case left :: right :: Nil =>
              for {
                left      <- pipelineOp(left)
                right     <- pipelineOp(right)
                leftMap   <- left.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
                rightMap  <- right.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
              } yield PipelineOp.Project(PipelineOp.Reshape(leftMap ++ rightMap)) :: (left.tail ++ right.tail) // TODO: Verify tails are empty

            case _ => None
          })

        case `Filter` =>           
          getOrFail("Expected pipeline op for set being filtered and selector for filter")(args match {
            case ops :: filter :: Nil => for {
              ops <- pipelineOp(ops)
              sel <- selector(filter) 
            } yield PipelineOp.Match(sel) :: ops

            case _ => None
          })

        case `Drop` =>
          getOrFail("Expected pipeline op for set being skipped and number to skip")(args match {
            case set :: count :: Nil => for {
              ops   <- pipelineOp(set)
              count <- constantLong(count)
            } yield PipelineOp.Skip(count) :: ops

            case _ => None
          })
        
        case `Take` =>
          getOrFail("Expected pipeline op for set being limited and number to limit")(args match {
            case set :: count :: Nil => for {
              ops   <- pipelineOp(set)
              count <- constantLong(count)
            } yield PipelineOp.Limit(count) :: ops

            case _ => None
          })

        case `GroupBy` => 
          type MapField[V] = Map[BsonField.Name, V]

          // MongoDB's $group cannot produce nested documents. So passes are required to support them.
          getOrFail("Expected (flat) projection pipeline op for set being grouped and expression op for value to group on")(args match {
            case set :: by :: Nil => for {
              ops     <-  pipelineOp(set) // TODO: Tail
              reshape <-  ops.headOption.collect { case PipelineOp.Project(PipelineOp.Reshape(map)) => map }
              grouped <-  Traverse[MapField].sequence(reshape.mapValues { 
                            case -\/(groupOp : ExprOp.GroupOp) => Some(groupOp)

                            case _ => None 
                          })
              by      <-  exprOp(by)
            } yield PipelineOp.Group(PipelineOp.Grouped(grouped), by) :: ops.tailOption.getOrElse(Nil)

            case _ => None
          })

        case `OrderBy` => {
          def invoke(v: (Term[LogicalPlan], Input, Output)): Option[(Func, List[Term[LogicalPlan]])] = v._1.unFix.fold(
            read      = _ => None,
            constant  = _ => None,
            join      = (_, _, _, _, _, _) => None,
            invoke    = (func, args) => Some(func -> args),
            free      = _ => None,
            let       = (_, _) => None
          )
        
          def invokeProject(v: (Term[LogicalPlan], Input, Output)): Option[(Term[LogicalPlan], String)] = for {
            (`ObjectProject`, args) <- invoke(v)
            obj = args(0)
            name <- constantStr(args(1), v._2, v._3)  // TODO: need to do something with in/out?
          } yield (obj, name)
          
          def invokeProjectArray(v: (Term[LogicalPlan], Input, Output)): Option[String] = for {
            (`MakeArray`, args) <- invoke(v)
            (obj, name) <- invokeProject(args(0), v._2, v._3)  // TODO: need to do something with in/out?
          } yield name
          
          def invokeProjectArrayConcat(v: (Term[LogicalPlan], Input, Output)): Option[NonEmptyList[String]] = {
            import Scalaz._
            val namesFromArray: Option[NonEmptyList[String]] = for {
              (`ArrayConcat`, args) <- invoke(v)
              names <- args.map(invokeProjectArrayConcat(_, v._2, v._3)).sequence  // TODO: need to do something with in/out?
              flatNames = names.flatMap(_.list)
            } yield NonEmptyList.nel(flatNames.head, flatNames.tail)
            namesFromArray.orElse(invokeProjectArray(v).map(NonEmptyList(_)))
          }
          
          getOrFail("Expected pipeline op for set being sorted and keys")(args match {
            case set :: keys :: Nil => for {
              ops <- pipelineOp(set)
              keyNames <- invokeProjectArrayConcat(keys)
              sortType: SortType = Ascending  // TODO: asc vs. desc
            } yield PipelineOp.Sort(keyNames.map(n => (BsonField.Name(n) -> sortType))) :: ops
            
            case _ => None
          })
        }
        
        case _ => nothing
      }
    } */
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
}
