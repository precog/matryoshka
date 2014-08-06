package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._

import collection.immutable.ListMap

import scalaz.{Free => FreeM, Node => _, _}
import scalaz.task.Task

import Scalaz._

object MongoDbPlanner extends Planner[Workflow] {
  import LogicalPlan._

  import slamdata.engine.analysis.fixplate._

  import agg._
  import array._
  import date._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  /**
   * This phase works bottom-up to assemble sequences of object dereferences
   * into the format required by MongoDB -- e.g. "foo.bar.baz".
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
  def FieldPhase[A]: PhaseE[LogicalPlan, Error, A, Option[BsonField]] = lpBoundPhaseE {
    type Output = Error \/ Option[BsonField]

    toPhaseE(Phase { (attr: Attr[LogicalPlan, A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(Term[LogicalPlan], Output)]) => {
        def nothing: Output = \/- (None)
        def emit(field: BsonField): Output = \/- (Some(field))
        
        def buildProject(parent: Option[BsonField], child: BsonField.Leaf) =
          emit(parent match {
            case Some(objAttr) => objAttr \ child
            case None          => child
          })

        node match {
          case ObjectProject((_, \/- (objAttrOpt)) :: (Constant(Data.Str(fieldName)), _) :: Nil) =>
            buildProject(objAttrOpt, BsonField.Name(fieldName))

          case ObjectProject(_) => -\/ (PlannerError.UnsupportedPlan(node))

          case ArrayProject((_, \/- (objAttrOpt)) :: (Constant(Data.Int(index)), _) :: Nil) =>
            buildProject(objAttrOpt, BsonField.Index(index.toInt))

          case ArrayProject(_) => -\/ (PlannerError.UnsupportedPlan(node))

          case _ => nothing
        }
      }
    }})
  }

  /**
   * The selector phase tries to turn expressions into MongoDB selectors -- i.e.
   * Mongo query expressions. Selectors are only used for the filtering pipeline
   * op, so it's quite possible we build more stuff than is needed (but it
   * doesn't matter, unneeded annotations will be ignored by the pipeline
   * phase).
   *
   * Like the expression op phase, this one requires bson field annotations.
   *
   * Most expressions cannot be turned into selector expressions without using
   * the "\$where" operator, which allows embedding JavaScript
   * code. Unfortunately, using this operator turns filtering into a full table
   * scan. We should do a pass over the tree to identify partial boolean
   * expressions which can be turned into selectors, factoring out the leftovers
   * for conversion using $where.
   */
  def SelectorPhase: PhaseE[LogicalPlan, Error, Option[BsonField], Option[Selector]] = lpBoundPhaseE {
    type Input = Option[BsonField]
    type Output = Option[Selector]

    liftPhaseE(Phase { (attr: Attr[LogicalPlan,Input]) =>
      scanPara2(attr) { (fieldAttr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        def emit(sel: Selector): Output = Some(sel)

        def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
          object IsBson {
            def unapply(v: Term[LogicalPlan]): Option[Bson] = Constant.unapply(v).flatMap(Bson.fromData(_).toOption)
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
            } yield Selector.Doc(ListMap(field -> Selector.Expr(f(value))))

          def stringOp(f: String => Selector.Condition) =
            for {
              (field, value) <- extractFieldAndSelector
              str <- value match { case Bson.Text(s) => Some(s); case _ => None }
            } yield (Selector.Doc(ListMap(field -> Selector.Expr(f(str)))))

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
              case (_, Some(f), _) :: (IsBson(lower), _, _) :: (IsBson(upper), _, _) :: Nil =>
                Some(Selector.And(
                  Selector.Doc(f -> Selector.Gte(lower)),
                  Selector.Doc(f -> Selector.Lte(upper))
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
          let       = (_, _, in) => in._3
        )
      }
    })
  }

  def PipelinePhase: PhaseE[LogicalPlan, Error, Option[Selector], Option[PipelineBuilder]] = lpBoundPhaseE {
    type Input  = Option[Selector]
    type Output = Error \/ Option[PipelineBuilder]

    import PipelineOp._

    def nothing = \/- (None)
    
    object HasSelector {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Selector] = v match {
        case Attr((sel, _), _) => sel
        case _ => None
      }
    }

    object HasLiteral {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Bson] = v match {
        case HasPipeline(p) => p.asLiteral.toOption.map(_.value)
      }
    }

    object HasPipeline {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[PipelineBuilder] = v.unFix.attr._2.toOption.flatten
    }

    val convertError = (e: Error) => e

    def emit[A](a: A): Error \/ A = \/- (a)

    def addOpSome(p: PipelineBuilder, op: ShapePreservingOp): Output = p.unify(PipelineBuilder.fromInit(op)) { (l, r) =>
      \/- (PipelineBuilder.fromExpr(l))
    }.bimap(convertError, Some.apply)

    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Output)]]): Output = {
      def funcError(msg: String) = {
        def funcFormatter[A](args: List[Attr[LogicalPlan, A]])(anns: List[(String, A => String)])(implicit slp: Show[LogicalPlan[_]]): (String => String) = {
          val labelWidth = anns.map(_._1.length).max + 2
          def pad(l: String) = l.padTo(labelWidth, " ").mkString
          def argSumm(n: Attr[LogicalPlan, A]) = 
            "    " + slp.show(n.unFix.unAnn) ::
            anns.map { case (label, f) => "      " + pad(label + ": ") + f(n.unFix.attr) }
          msg => (msg :: "  func: " + func.toString :: "  args:" :: args.flatMap(argSumm)).mkString("\n")
        }

        val ff = funcFormatter(args)(("selector" -> ((a: (Input, Output)) => a._1.shows)) ::
                                     ("pipeline" -> ((a: (Input, Output)) => a._2.shows)) :: Nil)

        -\/ (PlannerError.InternalError(ff(msg)))
      }

      def expr1(f: ExprOp => ExprOp): Output = {
        args match {
          case HasPipeline(p) :: Nil =>
            p.map(e => \/- (PipelineBuilder.fromExpr(f(e)))).bimap(convertError, Some.apply)

          case _ => funcError("Cannot compile expression because the subexpressions does not have a pipeline")
        }
      }

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output = {
        args match {
          case HasPipeline(p) :: Nil =>
            (for {
              p <- if (p.isGrouped) \/- (p) else p.groupBy(PipelineBuilder.fromExpr(ExprOp.Literal(Bson.Int32(1))))
              p <- p.reduce(f)
            } yield p).bimap(convertError, Some.apply)
        }
      }

      def mapExpr(p: PipelineBuilder)(f: ExprOp => ExprOp): Output = {
        p.map(e => \/- (PipelineBuilder.fromExpr(f(e)))).bimap(convertError, Some.apply)
      }

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output = {
        args match {
          case HasPipeline(p1) :: HasPipeline(p2) :: Nil =>
            p1.unify(p2) { (l, r) =>
              \/- (PipelineBuilder.fromExpr(f(l, r)))
            }.bimap(convertError, Some.apply)

          case _ => funcError("Cannot compile expression because one or both subexpressions do not have a pipeline")
        }
      }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output = {
        args match {
          case HasPipeline(p1) :: HasPipeline(p2) :: HasPipeline(p3) :: Nil =>
            val Root  = ExprOp.DocVar.ROOT()
            val Left  = BsonField.Name("left")
            val Right = BsonField.Name("right")

            (for {
              p12     <-  p1.unify(p2) { (l, r) =>
                            \/- (PipelineBuilder.fromExprs("left" -> l, "right" -> r))
                          }

              p123    <-  p12.unify(p3) { (l, r) =>
                            \/- (PipelineBuilder.fromExprs("left" -> l, "right" -> r))
                          }

              pfinal  <-  p123.map { root => 
                            \/- (PipelineBuilder.fromExpr(f(root \ Left \ Left, root \ Left \ Right, root \ Right))) 
                          }
            } yield pfinal).bimap(convertError, Some.apply)

          case _ => funcError("Cannot compile expression because one, both, or all subexpressions do not have a pipeline")
        }
      }

      func match {
        case `MakeArray` => 
          args match {
            case HasPipeline(pipe) :: Nil => 
              pipe.makeArray.bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile a MakeArray because a pipeline was not found")
          }

        case `MakeObject` =>
          args match {
            case HasLiteral(Bson.Text(name)) :: HasPipeline(pipe) :: Nil => 
              pipe.makeObject(name).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile a MakeObject because a literal and / or pipeline were not found")
          }
        
        case `ObjectConcat` =>
          args match {
            case HasPipeline(p1) :: HasPipeline(p2) :: Nil =>
              p1.objectConcat(p2).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile an ObjectConcat because both sides do not have pipelines")
          }
        
        case `ArrayConcat` =>
          args match {
            case HasPipeline(p1) :: HasPipeline(p2) :: Nil =>
              p1.arrayConcat(p2).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile an ArrayConcat because both do not have pipelines")
          }

        case `Filter` => 
          args match {
            case HasPipeline(p) :: HasSelector(q) :: Nil => addOpSome(p, Match(q))
            
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
            case HasPipeline(p1) :: HasPipeline(p2) :: Nil =>
              p1.groupBy(p2).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile GroupBy because a group or a group by expression could not be extracted")
          }

        case `OrderBy` => {
          args match {
            case HasPipeline(p1) :: HasPipeline(p2) :: Nil =>
              p1.sortBy(p2).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile OrderBy because cannot extract out a project and a project / expression")
          }
        }

        case `Like`       => nothing  // FIXME

        case `Add`        => expr2(ExprOp.Add.apply _)
        case `Multiply`   => expr2(ExprOp.Multiply.apply _)
        case `Subtract`   => expr2(ExprOp.Subtract.apply _)
        case `Divide`     => expr2(ExprOp.Divide.apply _)
        case `Modulo`     => expr2(ExprOp.Mod.apply _)

        case `Eq`         => expr2(ExprOp.Eq.apply _)
        case `Neq`        => expr2(ExprOp.Neq.apply _)
        case `Lt`         => expr2(ExprOp.Lt.apply _)
        case `Lte`        => expr2(ExprOp.Lte.apply _)
        case `Gt`         => expr2(ExprOp.Gt.apply _)
        case `Gte`        => expr2(ExprOp.Gte.apply _)

        case `Coalesce`   => expr2(ExprOp.IfNull.apply _)

        case `Concat`     => expr2(ExprOp.Concat(_, _, Nil))
        case `Lower`      => expr1(ExprOp.ToLower.apply _)
        case `Upper`      => expr1(ExprOp.ToUpper.apply _)
        case `Substring`  => expr3(ExprOp.Substr(_, _, _))
        
        case `Cond`       => expr3(ExprOp.Cond.apply _)


        case `Count`      => groupExpr1(_ => ExprOp.Count)
        case `Sum`        => groupExpr1(ExprOp.Sum.apply _)
        case `Avg`        => groupExpr1(ExprOp.Avg.apply _)
        case `Min`        => groupExpr1(ExprOp.Min.apply _)
        case `Max`        => groupExpr1(ExprOp.Max.apply _)

        case `ArrayLength` => 
          args match {
            case HasPipeline(p) :: HasLiteral(Bson.Int64(1)) :: Nil =>
              p.map(e => \/- (PipelineBuilder.fromExpr(ExprOp.Size(e)))).bimap(convertError, Some.apply)

            case _ => funcError("Cannot compile ArrayLength because cannot extract pipeline and / or literal number")
          }

        case `Extract`   => 
          args match {
            case HasLiteral(Bson.Text(field)) :: HasPipeline(p) :: Nil =>
              field match {
                case "century"      =>
                  mapExpr(p) { v => 
                    ExprOp.Divide(
                      ExprOp.Year(v),
                      ExprOp.Literal(Bson.Int32(100))
                    )
                  }
                case "day"          => mapExpr(p)(ExprOp.DayOfMonth(_))
                // FIXME: `dow` returns the wrong value for Sunday
                case "dow"          => mapExpr(p)(ExprOp.DayOfWeek(_))
                case "doy"          => mapExpr(p)(ExprOp.DayOfYear(_))
                case "hour"         => mapExpr(p)(ExprOp.Hour(_))
                case "isodow"       => mapExpr(p)(ExprOp.DayOfWeek(_))
                case "microseconds" =>
                  mapExpr(p) { v =>
                    ExprOp.Multiply(
                      ExprOp.Millisecond(v),
                      ExprOp.Literal(Bson.Int32(1000))
                    )
                  }
                case "millennium"   =>
                  mapExpr(p) { v =>
                    ExprOp.Divide(
                      ExprOp.Year(v),
                      ExprOp.Literal(Bson.Int32(1000))
                    )
                  }
                case "milliseconds" => mapExpr(p)(ExprOp.Millisecond(_))
                case "minute"       => mapExpr(p)(ExprOp.Minute(_))
                case "month"        => mapExpr(p)(ExprOp.Month(_))
                case "quarter"      =>
                  mapExpr(p) { v =>
                    ExprOp.Add(
                      ExprOp.Divide(
                        ExprOp.DayOfYear(v),
                        ExprOp.Literal(Bson.Int32(92))
                      ),
                      ExprOp.Literal(Bson.Int32(1))
                    )
                  }
                case "second"       => mapExpr(p)(ExprOp.Second(_))
                case "week"         => mapExpr(p)(ExprOp.Week(_))
                case "year"         => mapExpr(p)(ExprOp.Year(_))
                case _              => funcError("Cannot compile Extract: unknown time period '" + field + "'")
              }

            case _ => funcError("Cannot compile Extract")
          }

        case `Between` => expr3((x, l, u) => ExprOp.And(NonEmptyList.nel(ExprOp.Gte(x, l), ExprOp.Lte(x, u) :: Nil)))

        case `ObjectProject` => 
          args match {
            case HasPipeline(p) :: HasLiteral(Bson.Text(name)) :: Nil =>
              p.projectField(name).bimap(convertError, Some.apply)

            case _ => funcError("Cannot project -- missing pipeline or literal text")
          }

        case `ArrayProject` => 
          args match {
            case HasPipeline(p) :: HasLiteral(Bson.Int64(index)) :: Nil =>
              p.projectIndex(index.toInt).bimap(convertError, Some.apply)

            case _ => funcError("Cannot project -- missing pipeline or literal text")
          }

        case `Squash` =>
          args match {
            case HasPipeline(p) :: Nil => emit(Some(p))

            case _ => funcError("Cannot compile Squash without pipeline")
          }

        case _ => funcError("Function " + func + " cannot be compiled to a pipeline op")
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)
      // println(RenderTree.showGraphviz(attr))

      val attr2 = scanPara0(attr) { (orig: Attr[LogicalPlan, Input], node: LogicalPlan[Attr[LogicalPlan, (Input, Output)]]) =>
        node.fold[Output](
          read      = _ => \/- (Some(PipelineBuilder.empty)),
          constant  = d => Bson.fromData(d).bimap(
                        _ => PlannerError.InternalError("Cannot convert literal data to BSON: " + d),
                        b => Some(PipelineBuilder.fromExpr(ExprOp.Literal(b)))
                      ),
          join      = (_, _, _, _, _, _) => nothing,
          invoke    = invoke(_, _),
          free      = _ => nothing,
          let       = (_, _, in) => in.unFix.attr._2
        )
      }

      // println(Show[Attr[LogicalPlan, Output]].show(attr2).toString)
      // println(RenderTree.showGraphviz(attr2))

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
        let       = (_, _, _) => Nil
      )
    }
  }

  val AllPhases = (FieldPhase[Unit]) >>> SelectorPhase >>> PipelinePhase

  def plan(logical: Term[LogicalPlan]): Error \/ Workflow = {
    import WorkflowTask._
    import ExprOp.DocVar

    def trivial(p: Path) =
      Collection.fromPath(p).fold(
        e => -\/ (PlannerError.InternalError(e.message)),
        col => \/- (WorkflowTask.ReadTask(col)))

    def nonTrivial: Error \/ Workflow = {
      val paths = collectReads(logical)

      AllPhases(attrUnit(logical)).map(_.unFix.attr).flatMap { pbOpt =>
        paths match {
          case path :: Nil => 
            trivial(path).flatMap { read =>
              pbOpt match {
                case Some(PipelineBuilder(Nil, DocVar.ROOT(None), SchemaChange.Init, Nil)) => \/- (Workflow(read))

                case Some(builder) => 
                  builder.build.bimap(
                    e => PlannerError.InternalError(e.message), 
                    b => Workflow(WorkflowTask.PipelineTask(read, b))
                  )

                case None => -\/ (PlannerError.InternalError("The plan cannot yet be compiled to a MongoDB workflow"))
              }
            }

          case _ => -\/ (PlannerError.InternalError("Pipeline compiler requires a single source for reading data from"))
        }
      }
    }

    logical.unFix.fold(
      read      = p => trivial(p).map(Workflow(_)),
      constant  = _ => nonTrivial,
      join      = (_, _, _, _, _, _) => nonTrivial,
      invoke    = (_, _) => nonTrivial,
      free      = _ => nonTrivial,
      let       = (_, _, _) => nonTrivial
    )
  }
}
