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
  import Extractors._

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
    
    liftPhaseE(Phase { (attr: Attr[LogicalPlan, A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(Term[LogicalPlan], Output)]) =>
        node match {
          case ObjectProject((_, objAttrOpt) :: (IsConstant(Data.Str(fieldName)), _) :: Nil) => 
            Some(objAttrOpt match {
              case Some(objAttr) => objAttr \ BsonField.Name(fieldName)

              case None => BsonField.Name(fieldName)
            })
            
          // TODO: Make pattern matching safer (i.e. generate error if pattern not matched):
          // case ObjectProject(_) => error(...)
            
          // Mongo treats array derefs the same as object derefs.
          case ArrayProject((_, objAttrOpt) :: (IsConstant(Data.Int(index)), _) :: Nil) =>
            Some(objAttrOpt match {
              case Some(objAttr) => objAttr \ BsonField.Name(index.toString)

              case None => BsonField.Name(index.toString)
            })
            
          // TODO: Make pattern matching safer (i.e. generate error if pattern not matched):
          // case ArrayProject(_) => error(...)
          
          case _ => None
        }
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

    toPhaseE(Phase { (attr: Attr[LogicalPlan, Option[BsonField]]) =>
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

            case `Between`  => {
              val first :: array :: Nil = args
              val xOpt: Option[ExprOp] = first.fold(e => None, x => x)
              xOpt.map(x => {
                // TODO: extract min and max from the array, which itself is not annotated
                val min = ExprOp.Literal(Bson.Int32(111))
                val max = ExprOp.Literal(Bson.Int32(999))
                emit(ExprOp.And(NonEmptyList.nel(
                              ExprOp.Gte(x, min),
                              ExprOp.Lte(x, max) ::
                              Nil
                            )))
              }).getOrElse(nothing)
            }

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
          object IsBson {
            def unapply(v: Term[LogicalPlan]): Option[Bson] = IsConstant.unapply(v).flatMap(Bson.fromData(_).toOption)
          }
          
          /**
           * Attempts to extract a BsonField annotation and a Bson value from
           * an argument list of length two (in any order).
           */
          def extractFieldAndSelector: Option[(BsonField, Bson)] = args match {
            case (IsBson(v1), _, _) :: (_, Some(f2), _) :: Nil => Some(f2 -> v1)
            case (_, Some(f1), _) :: (IsBson(v2), _, _) :: Nil => Some(f1 -> v2)
            case _                                             => None
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

            case `Between`  => args match {
              case (_, Some(f), _) :: (IsArray(IsBson(min) :: IsBson(max) :: Nil), _, _) :: Nil => 
                Some(Selector.And(
                    Selector.Doc(f -> Selector.Gte(min)),
                    Selector.Doc(f -> Selector.Lte(max))
                  ))
                  
              case _ => None
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
    
    object HasSelector {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Selector] = v match {
        case HasAnn(((sel, _), _)) => sel
        case _ => None
      }
    }

    object HasExpr {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[ExprOp] = v match {
        case HasPipeline(_) => None
        case HasAnn(((_, expr), _)) => expr
        case _ => None
      }
    }

    object HasGroupOp {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[ExprOp.GroupOp] = v match {
        case HasExpr(x : ExprOp.GroupOp) => Some(x)
        case _ => None
      }
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
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[String] = HasLiteral.unapply(node) collect { 
        case Bson.Text(str) => str
      }
    }

    object HasPipeline {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[PipelineBuilder] = {
        val defaultCase = v.unFix.attr._2.toOption.flatten
        defaultCase.orElse(v match {
          case IsReadAttr(_) => Some(PipelineBuilder.empty)
          case _ => None
        })
      }
    }

    object LeadingProject {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[(Project, PipelineBuilder)] = v match {
        case HasPipeline(p) => p.buffer.find(_.isNotShapePreservingOp).collect {
        case p @ Project(_) => p
      }.headOption.map(_ -> p)

        case _ => None
      }
    }

    object LeadingGroup {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[(Group, PipelineBuilder)] = v match {
        case HasPipeline(p) => p.buffer.find(_.isNotShapePreservingOp).collect {
          case g @ Group(_, _) => g
        }.headOption.map(_ -> p)
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
          case IsObjectAttr((HasStringConstant("key"), HasField(key)) ::
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
          case IsArrayAttr(AllSortKeys(k :: ks)) => Some(NonEmptyList.nel(k, ks))
          case _ => None
        }
      }
    }

    def emit[A](a: A): PlannerError \/ A = \/- (a)

    def emitSome[A](a: A): PlannerError \/ Option[A] = \/- (Some(a))

    def error[A](msg: String): PlannerError \/ A = -\/ (PlannerError.InternalError(msg))

    def projField(ts: (String, ExprOp \/ Reshape)*): Reshape = Reshape.Doc(ts.map(t => BsonField.Name(t._1) -> t._2).toMap)
    def projIndex(ts: (Int,    ExprOp \/ Reshape)*): Reshape = Reshape.Arr(ts.map(t => BsonField.Index(t._1) -> t._2).toMap)

    val convertError = (e: MergePatchError) => PlannerError.InternalError(e.message)

    def merge(pipe1: PipelineBuilder, pipe2: PipelineBuilder): PlannerError \/ PipelineBuilder = pipe1.merge(pipe2).leftMap(convertError)

    def mergeSome(pipe1: PipelineBuilder, pipe2: PipelineBuilder): Output = merge(pipe1, pipe2).map(Some.apply)

    def addOp(pipe: PipelineBuilder, op: PipelineOp): PlannerError \/ PipelineBuilder = (pipe + op).leftMap(convertError)

    def addAllOps(pipe: PipelineBuilder, ops: List[PipelineOp]): PlannerError \/ PipelineBuilder = (pipe ++ ops).leftMap(convertError)

    def addAllOpsSome(pipe: PipelineBuilder, ops: List[PipelineOp]): Output = addAllOps(pipe, ops).map(Some.apply)

    def addOpSome(pipe: PipelineBuilder, op: PipelineOp): Output = addOp(pipe, op).map(Some.apply)

    def promoteExpr(p1: PipelineBuilder, expr: ExprOp)(f: ExprOp.DocVar => PlannerError \/ PipelineOp): Output = {
      val ExprFieldName = "__sd_expr"
      val ExprField = BsonField.Name(ExprFieldName)

      val p2 = PipelineBuilder(Project(Reshape.Doc(Map(ExprField -> -\/(expr)))))

      for {
        t <- p1.merge0(p2).leftMap(convertError)

        (merged, lp, rp) = t

        newRef = rp.applyVar(ExprOp.DocVar.ROOT(ExprField)) // Where'd it go?

        finalOp <- f(newRef)

        r <- (merged + finalOp).leftMap(convertError)
      } yield Some(r)
    } 

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

    def sortBy(p: Project, by: ExprOp \/ Reshape): (Project, Sort) = {
      val (sortFields, r) = p.shape match {
        case Reshape.Doc(m) => 
          val field = BsonField.genUniqName(m.keys)

          NonEmptyList(field -> Ascending) -> Reshape.Doc(m + (field -> by))

        case Reshape.Arr(m) => 
          val field = BsonField.genUniqIndex(m.keys)

          NonEmptyList(field -> Ascending) -> Reshape.Arr(m + (field -> by))
      }

      Project(r) -> Sort(sortFields)
    }

    val GroupBy1 = -\/ (ExprOp.Literal(Bson.Int64(1)))

    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Output)]]): Output = {
      def funcError(msg: String) = {
        def funcFormatter[A](func: Func, args: List[Attr[LogicalPlan, A]])(anns: List[(String, A => String)])(implicit slp: Show[LogicalPlan[_]]): (String => String) = {
          val labelWidth = anns.map(_._1.length).max + 2
          def pad(l: String) = l.padTo(labelWidth, " ").mkString
          def argSumm(n: Attr[LogicalPlan, A]) = 
            "    " + slp.show(n.unFix.unAnn) ::
            anns.map { case (label, f) => "      " + pad(label + ": ") + f(n.unFix.attr) }
          msg => (msg :: "  func: " + func.toString :: "  args:" :: args.flatMap(argSumm)).mkString("\n")
        }

        val ff = funcFormatter(func, args)(("selector" -> ((a: (Input, Output)) => a._1._1.shows)) ::
                                         ("expr"     -> ((a: (Input, Output)) => a._1._2.shows)) ::
                                         ("pipeline" -> ((a: (Input, Output)) => a._2.shows)) :: Nil)

        -\/ (PlannerError.InternalError(ff(msg)))
      }

      func match {
        case `MakeArray` => 
          args match {
            case LeadingProject(proj, pipe) :: Nil => 
              addOpSome(pipe, proj.id.nestIndex(0))

            case HasGroupOp(e) :: Nil => 
              emitSome(PipelineBuilder(Group(Grouped(Map(BsonField.Index(0) -> e)), GroupBy1)))

            case HasExpr(e) :: Nil => 
              emitSome(PipelineBuilder(Project(projIndex(0 -> -\/(e)))))

            case _ => funcError("Cannot compile a MakeArray because neither an expression nor a reshape pipeline were found")
          }

        case `MakeObject` =>
          args match {
            case HasLiteral(Bson.Text(name)) :: LeadingProject(proj, pipe) :: Nil => 
              addOpSome(pipe, proj.id.nestField(name))

            case HasLiteral(Bson.Text(name)) :: HasGroupOp(e) :: Nil => 
              emitSome(PipelineBuilder(Group(Grouped(Map(BsonField.Name(name) -> e)), GroupBy1)))

            case HasLiteral(Bson.Text(name)) :: HasExpr(e) :: Nil => 
              emitSome(PipelineBuilder(Project(projField(name -> -\/(e)))))

            case _ => funcError("Cannot compile a MakeObject because neither an expression nor a reshape pipeline were found")
          }
        
        case `ObjectConcat` =>
          args match {
            case LeadingProject(_, pipe1) :: LeadingProject(_, pipe2) :: Nil => mergeSome(pipe1, pipe2)

            case _ => funcError("Cannot compile an ObjectConcat because both sides are not projects")
          }
        
        case `ArrayConcat` =>
          args match {
            case LeadingProject(_, pipe1) :: LeadingProject(_, pipe2) :: Nil => mergeSome(pipe1, pipe2)

            case _ => funcError("Cannot compile an ArrayConcat because both sides are not projects building arrays")
          }

        case `Filter` => 
          args match {
            case HasPipeline(p) :: HasSelector(q) :: Nil => mergeSome(p, PipelineBuilder(Match(q)))

            case _ => funcError("Cannot compile a Filter because the set has no pipeline or the predicate has no selector")
          }

        case `Drop` =>
          args match {
            case HasPipeline(p) :: HasLiteral(Bson.Int64(v)) :: Nil => addOpSome(p, Skip(v))

            case _ => funcError("Cannot compile Drop because the set has no pipeline or number has no literal")
          }
        
        case `Take` => 
          args match {
            case HasPipeline(p) :: HasLiteral(Bson.Int64(v)) :: Nil => addOpSome(p, Limit(v))

            case _ => funcError("Cannot compile Take because the set has no pipeline or number has no literal")
          }

        case `GroupBy` =>
          args match {
            case LeadingProject(proj1, pipe1) :: LeadingProject(proj2, pipe2) :: Nil => 
              ???

            case LeadingProject(proj, pipe) :: HasExpr(e) :: Nil => 
              ???

            case _ => funcError("Cannot compile GroupBy because a projection or a projection / expression could not be extracted")
          }

        case `OrderBy` =>
          // TODO: Support descending and sorting fields individually
          args match {
            case LeadingProject(_, pipe) :: HasExpr(e) :: Nil =>
              promoteExpr(pipe, e) { docVar =>
                \/- (Sort(NonEmptyList(docVar.field -> Ascending)))
              }

            case LeadingProject(proj1, pipe1) :: LeadingProject(proj2, pipe2) :: Nil =>
              val (proj, sort) = sortBy(proj1.id, \/- (proj2.id.shape))

              for {
                mpipe <- merge(pipe1, pipe2)
                mpipe <- addAllOpsSome(mpipe, proj :: sort :: Nil)
              } yield mpipe

            case HasPipeline(p) :: HasSortFields(fields) :: Nil =>
              // TODO: Can generalize this simply by adding patches to PipelineBuilder so we can propagate
              // rename / nest information through the whole tree.
              addOpSome(p, Sort(fields)) 

            case _ => funcError("Cannot compile OrderBy because cannot extract out a project and a project / expression")
          }

        case `ObjectProject` =>
          args match {
            case LeadingProject(proj, pipe) :: HasLiteral(Bson.Text(field)) :: Nil =>
              proj.id.field(field).map { _ fold(
                  _ => error("Çannot deref into expression yet"), // FIXME!!! This indicates a deref resulted in an expression which cannot be translated into pipeline op.
                  projField => addOpSome(pipe, projField)
                )
              }.getOrElse(funcError("No field of value " + field + " could be found"))

            case _ => funcError("Unknown format for object project")
          }
        
        case `ArrayProject` =>
          args match {
            case LeadingProject(proj, pipe) :: HasLiteral(Bson.Int64(idx)) :: Nil =>
              proj.id.index(idx.toInt).map { _ fold(
                  _ => error("Çannot deref into expression yet"), // FIXME!!! This indicates a deref resulted in an expression which cannot be translated into pipeline op.
                  projIndex => addOpSome(pipe, projIndex)
                )
              }.getOrElse(funcError("No index of value " + idx + " could be found"))
          }

        case `Between` => nothing

        case _ => funcError("Function " + func + " cannot be compiled to a pipeline op")
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)

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
                          val ps: List[Option[PipelineBuilder]] = vs.map(_.unFix.attr).map {
                            case ((_, _), \/-(Some(pOp))) => Some(pOp)
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
