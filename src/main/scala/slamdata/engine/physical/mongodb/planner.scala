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
  def FieldPhase[A]: PhaseE[LogicalPlan, PlannerError, A, Option[BsonField]] = lpBoundPhaseE {
    type Output = PlannerError \/ Option[BsonField]

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
   * This phase builds up expression operations from field attributes.
   *
   * As it works its way up the tree, at some point, it will reach a place where
   * the value cannot be computed as an expression operation. The phase will
   * produce None at these points. Further up the tree from such a position, it
   * may again be possible to build expression operations, so this process will
   * naturally result in spans of expressions alternating with spans of nothing
   * (i.e. None).
   *
   * The "holes" represent positions where a pipeline operation or even a
   * workflow task is required to compute the given expression.
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

          def invoke3(f: (ExprOp, ExprOp, ExprOp) => ExprOp) = {
            val x :: y :: z :: Nil = args

            (x |@| y |@| z)(f)
          }

          func match {
            case `Add`      => invoke2(ExprOp.Add.apply _)
            case `Multiply` => invoke2(ExprOp.Multiply.apply _)
            case `Subtract` => invoke2(ExprOp.Subtract.apply _)
            case `Divide`   => invoke2(ExprOp.Divide.apply _)
            case `Modulo`   => invoke2(ExprOp.Mod.apply _)

            case `Eq`       => invoke2(ExprOp.Eq.apply _)
            case `Neq`      => invoke2(ExprOp.Neq.apply _)
            case `Lt`       => invoke2(ExprOp.Lt.apply _)
            case `Lte`      => invoke2(ExprOp.Lte.apply _)
            case `Gt`       => invoke2(ExprOp.Gt.apply _)
            case `Gte`      => invoke2(ExprOp.Gte.apply _)
            case `Cond`     => invoke3(ExprOp.Cond.apply _)
            case `Coalesce` => invoke2(ExprOp.IfNull.apply _)

            case `Concat`    => invoke2(ExprOp.Concat(_, _, Nil))
            case `Substring` => invoke3(ExprOp.Substr(_, _, _))
            case `Lower`     => invoke1(ExprOp.ToLower.apply _)
            case `Upper`     => invoke1(ExprOp.ToUpper.apply _)

            case `ArrayLength` => args match {
              case \/-(Some(arr)) :: \/-(Some(ExprOp.Literal(Bson.Int64(1)))) :: Nil =>
                emit(ExprOp.Size(arr))
              case _ => nothing
            }

            case `Extract`   => {
              val field :: date :: Nil = args

              def simpleEx(f: ExprOp => ExprOp) =
                date.map(_.map(f))

              field match {
                case \/-(Some(ExprOp.Literal(Bson.Text(value)))) =>
                  value match {
                    case "century"      =>
                      simpleEx(ExprOp.Year.apply _).map(_.map(
                        ExprOp.Divide(
                          _,
                          ExprOp.Literal(Bson.Int32(100)))))
                    case "day"          => simpleEx(ExprOp.DayOfMonth.apply _)
                    // FIXME: `dow` returns the wrong value for Sunday
                    case "dow"          => simpleEx(ExprOp.DayOfWeek.apply _)
                    case "doy"          => simpleEx(ExprOp.DayOfYear.apply _)
                    case "hour"         => simpleEx(ExprOp.Hour.apply _)
                    case "isodow"       => simpleEx(ExprOp.DayOfWeek.apply _)
                    case "microseconds" =>
                      simpleEx(ExprOp.Millisecond.apply _).map(_.map(
                        ExprOp.Multiply(
                          _,
                          ExprOp.Literal(Bson.Int32(1000)))))
                    case "millennium"   =>
                      simpleEx(ExprOp.Year.apply _).map(_.map(
                        ExprOp.Divide(
                          _,
                          ExprOp.Literal(Bson.Int32(1000)))))
                    case "milliseconds" => simpleEx(ExprOp.Millisecond.apply _)
                    case "minute"       => simpleEx(ExprOp.Minute.apply _)
                    case "month"        => simpleEx(ExprOp.Month.apply _)
                    case "quarter"      =>
                      simpleEx(ExprOp.DayOfYear.apply _).map(_.map(field =>
                        ExprOp.Add(
                          ExprOp.Divide(
                            field,
                            ExprOp.Literal(Bson.Int32(92))),
                          ExprOp.Literal(Bson.Int32(1)))))
                    case "second"       => simpleEx(ExprOp.Second.apply _)
                    case "week"         => simpleEx(ExprOp.Week.apply _)
                    case "year"         => simpleEx(ExprOp.Year.apply _)
                    case _              => nothing
                  }
                  case _ => nothing
              }
            }

            case `Count`    => emit(ExprOp.Count)
            case `Sum`      => invoke1(ExprOp.Sum.apply _)
            case `Avg`      => invoke1(ExprOp.Avg.apply _)
            case `Min`      => invoke1(ExprOp.Min.apply _)
            case `Max`      => invoke1(ExprOp.Max.apply _)

            case `Between`  => args match {
              case \/- (Some(x)) :: \/- (Some(lower)) :: \/- (Some(upper)) :: Nil =>
                emit(ExprOp.And(NonEmptyList.nel(
                  ExprOp.Gte(x, lower),
                  ExprOp.Lte(x, upper) ::
                  Nil
                )))

              case _ => nothing
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
          let       = (_, _, in) => in
        )
      }
    })
  }

  private type EitherPlannerError[A] = PlannerError \/ A

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
  def SelectorPhase: PhaseE[LogicalPlan, PlannerError, Option[BsonField], Option[Selector]] = lpBoundPhaseE {
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

  private def getOrElse[A, B](b: B)(a: Option[A]): B \/ A = a.map(\/- apply).getOrElse(-\/ apply b)

  def PipelinePhase: PhaseE[LogicalPlan, PlannerError, (Option[Selector], Option[ExprOp]), Option[PipelineBuilder]] = lpBoundPhaseE {
    type Input  = (Option[Selector], Option[ExprOp])
    type Output = PlannerError \/ Option[PipelineBuilder]

    import PipelineOp._

    def nothing = \/- (None)
    
    object HasSelector {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[Selector] = v match {
        case Attr(((sel, _), _), _) => sel
        case _ => None
      }
    }

    object HasExpr {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[ExprOp] = v.unFix.attr._1._2
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
          case Read.Attr(_) => Some(PipelineBuilder.empty)
          case _ => None
        })
      }
    }

    object IsSortKey {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[(ExprOp, Option[PipelineBuilder], SortType)] =
        node match {
          case MakeObjectN.Attr((HasStringConstant("key"), keyAttr) ::
                                (HasStringConstant("order"), HasStringConstant(orderStr)) :: 
                                Nil)
                  => {
                    val pipe = keyAttr match {
                      case HasPipeline(pipe) => Some(pipe)
                      case _ => None
                    }
                    keyAttr match {
                      case HasExpr(key) =>
                        Some((key, pipe, (if (orderStr == "ASC") Ascending else Descending)))

                      case _ => None
                    }
                  }
                  
           case _ => None
        }
    }
    
    object AllSortKeys {
      def unapply(args: List[Attr[LogicalPlan, (Input, Output)]]): Option[List[(ExprOp, Option[PipelineBuilder], SortType)]] = 
        args.map(IsSortKey.unapply(_)).sequenceU
    }

    object HasSortKeys {
      def unapply(v: Attr[LogicalPlan, (Input, Output)]): Option[NonEmptyList[(ExprOp, Option[PipelineBuilder], SortType)]] = {
        v match {
          case MakeArrayN.Attr(AllSortKeys(k :: ks)) => Some(NonEmptyList.nel(k, ks))
          case _ => None
        }
      }
    }

    val convertError = (e: Error) => PlannerError.InternalError(e.message)

    def emit[A](a: A): PlannerError \/ A = \/- (a)

    def emitSome[A](a: A): PlannerError \/ Option[A] = emit(Some(a))

    def addOpSome(p: PipelineBuilder, op: ShapePreservingOp): Output = p.unify(PipelineBuilder.fromInit(op)) { (l, r) =>
      \/- (PipelineBuilder.fromExpr(l))
    }.bimap(convertError, Some.apply)

    def error[A](msg: String): PlannerError \/ A = -\/ (PlannerError.InternalError(msg))

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

        val ff = funcFormatter(args)(("selector" -> ((a: (Input, Output)) => a._1._1.shows)) ::
                                     ("expr"     -> ((a: (Input, Output)) => a._1._2.shows)) ::
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
            p1.unify(p2) { (l, m) =>
              val Root  = ExprOp.DocVar.ROOT()
              val Left  = BsonField.Name("left")
              val Right = BsonField.Name("right")

              val leftRef  = Root \ Left \ Left
              val midRef   = Root \ Left \ Right
              val rightRef = Root \ Right

              for {
                left  <- PipelineBuilder.fromExpr(l).makeObject("left")
                right <- PipelineBuilder.fromExpr(m).makeObject("right")
                p12   <- left.objectConcat(right)
                left  <- p12.makeObject("left")
                right <- p3.makeObject("right")
                p123  <- left.objectConcat(right)
              } yield PipelineBuilder.fromExpr(f(leftRef, midRef, rightRef))
            }.bimap(convertError, Some.apply)

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
            case _ => funcError("Cannot compile GroupBy because a group or a group by expression could not be extracted")
          }

        case `OrderBy` => {
          args match {
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


        case `Count`      => expr1(_ => ExprOp.Count)
        case `Sum`        => expr1(ExprOp.Sum.apply _)
        case `Avg`        => expr1(ExprOp.Avg.apply _)
        case `Min`        => expr1(ExprOp.Min.apply _)
        case `Max`        => expr1(ExprOp.Max.apply _)

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
                  expr1 { v => 
                    ExprOp.Divide(
                      ExprOp.Year(v),
                      ExprOp.Literal(Bson.Int32(100))
                    )
                  }
                case "day"          => expr1(ExprOp.DayOfMonth(_))
                // FIXME: `dow` returns the wrong value for Sunday
                case "dow"          => expr1(ExprOp.DayOfWeek(_))
                case "doy"          => expr1(ExprOp.DayOfYear(_))
                case "hour"         => expr1(ExprOp.Hour(_))
                case "isodow"       => expr1(ExprOp.DayOfWeek(_))
                case "microseconds" =>
                  expr1 { v =>
                    ExprOp.Multiply(
                      ExprOp.Millisecond(v),
                      ExprOp.Literal(Bson.Int32(1000))
                    )
                  }
                case "millennium"   =>
                  expr1 { v =>
                    ExprOp.Divide(
                      ExprOp.Year(v),
                      ExprOp.Literal(Bson.Int32(1000))
                    )
                  }
                case "milliseconds" => expr1(ExprOp.Millisecond(_))
                case "minute"       => expr1(ExprOp.Minute(_))
                case "month"        => expr1(ExprOp.Month(_))
                case "quarter"      =>
                  expr1 { v =>
                    ExprOp.Add(
                      ExprOp.Divide(
                        ExprOp.DayOfYear(v),
                        ExprOp.Literal(Bson.Int32(92))
                      ),
                      ExprOp.Literal(Bson.Int32(1))
                    )
                  }
                case "second"       => expr1(ExprOp.Second(_))
                case "week"         => expr1(ExprOp.Week(_))
                case "year"         => expr1(ExprOp.Year(_))
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

        case _ => funcError("Function " + func + " cannot be compiled to a pipeline op")
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      // println(Show[Attr[LogicalPlan, Input]].show(attr).toString)
      // println(RenderTree.showGraphviz(attr))

      val attr2 = scanPara0(attr) { (orig: Attr[LogicalPlan, Input], node: LogicalPlan[Attr[LogicalPlan, (Input, Output)]]) =>
        val (optSel, optExprOp) = orig.unFix.attr

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
      let       = (_, _, _) => nonTrivial
    )
  }
}
