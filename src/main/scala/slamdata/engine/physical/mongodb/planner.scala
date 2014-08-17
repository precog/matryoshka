package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._

import collection.immutable.ListMap

import scalaz.{Free => FreeM, Node => _, _}
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
    type Ann    = Attr[LogicalPlan, (Input, Output)]

    import PipelineOp._
    import PlannerError._

    object HasData {
      def unapply(node: Attr[LogicalPlan, (Input, Output)]): Option[Data] = node match {
        case LogicalPlan.Constant.Attr(data) => Some(data)
        case _ => None
      }
    }

    object IsSortKey {
      def unapply(node: Ann): Option[SortType] = 
        node match {
          case MakeObjectN.Attr((HasData(Data.Str("key")), _) ::
                                (HasData(Data.Str("order")), HasData(Data.Str("ASC"))) :: Nil) => Some(Ascending)
          case MakeObjectN.Attr((HasData(Data.Str("key")), _) ::
                                (HasData(Data.Str("order")), HasData(Data.Str("DESC"))) :: Nil) => Some(Descending)
                  
           case _ => None
        }
    }

    object HasSortKeys {
      def unapply(v: Ann): Option[List[SortType]] = {
        v match {
          case MakeArrayN.Attr(array) => array.map(IsSortKey.unapply(_)).sequenceU
          case _ => None
        }
      }
    }

    def emit[A](a: A): Error \/ A = \/- (a)

    def addOpSome(p: PipelineBuilder, op: ShapePreservingOp): Output = (p >>> op).rightMap(Some.apply)

    def invoke(func: Func, args: List[Ann]): Output = {
      val HasSelector: Ann => Error \/ Selector = {
        case Attr((Some(sel), _), _) => \/- (sel)
        case _ => -\/ (FuncApply(func, "selector", "none"))
      }

      val HasPipeline: Ann => Error \/ PipelineBuilder = 
        _.unFix.attr._2.toOption.flatten.map(\/- apply).getOrElse(-\/ (FuncApply(func, "pipeline", "nothing")))

      val HasLiteral: Ann => Error \/ Bson = HasPipeline(_).flatMap { p =>
        p.asLiteral match {
          case Some(ExprOp.Literal(value)) => \/- (value)
          case _ => -\/ (FuncApply(func, "literal", p.toString))
        }
      }

      val HasInt64: Ann => Error \/ Long = HasLiteral(_).flatMap {
        case Bson.Int64(v) => \/- (v)
        case x => -\/ (FuncApply(func, "64-bit integer", x.toString))
      }

      val HasText: Ann => Error \/ String = HasLiteral(_).flatMap {
        case Bson.Text(v) => \/- (v)
        case x => -\/ (FuncApply(func, "text", x.toString))
      }

      def Arity1[A](f: Ann => (Error \/ A)): Error \/ A = args match {
        case a1 :: Nil => f(a1)
        case _ => -\/ (FuncArity(func, 1))
      }

      def Arity2[A, B](f1: Ann => (Error \/ A), f2: Ann => (Error \/ B)): Error \/ (A, B) = args match {
        case a1 :: a2 :: Nil => (f1(a1) |@| f2(a2))((_, _))

        case _ => -\/ (FuncArity(func, 2))
      }

      def Arity3[A, B, C](f1: Ann => (Error \/ A), f2: Ann => (Error \/ B), f3: Ann => (Error \/ C)): Error \/ (A, B, C) = args match {
        case a1 :: a2 :: a3 :: Nil => (f1(a1) |@| f2(a2) |@| f3(a3))((_, _, _))

        case _ => -\/ (FuncArity(func, 3))
      }

      def expr1(f: ExprOp => ExprOp): Output = Arity1(HasPipeline).flatMap {
        _.expr1(e => \/- (f(e))).rightMap(Some.apply)
      }

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output = Arity1(HasPipeline).flatMap { p =>    
        (for {
          p <- if (p.isGrouped) \/- (p) else p.groupBy(PipelineBuilder.fromExpr(ExprOp.Literal(Bson.Int32(1))))
          p <- p.reduce(f)
        } yield p).rightMap(Some.apply)
      }

      def mapExpr(p: PipelineBuilder)(f: ExprOp => ExprOp): Output = {
        p.expr1(e => \/- (f(e))).rightMap(Some.apply)
      }

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output = Arity2(HasPipeline, HasPipeline).flatMap {
        case (p1, p2) =>
          p1.expr2(p2) { (l, r) =>
            \/- (f(l, r))
          }.rightMap(Some.apply)
      }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output = Arity3(HasPipeline, HasPipeline, HasPipeline).flatMap { 
        case (p1, p2, p3) => p1.expr3(p2, p3)((a, b, c) => \/- (f(a, b, c))).rightMap(Some.apply)
      }

      func match {
        case `MakeArray` => Arity1(HasPipeline).flatMap(_.makeArray.rightMap(Some.apply))

        case `MakeObject` =>
          Arity2(HasText, HasPipeline).flatMap {
            case (name, pipe) => pipe.makeObject(name).rightMap(Some.apply)
          }
        
        case `ObjectConcat` =>
          Arity2(HasPipeline, HasPipeline).flatMap {
            case (p1, p2) => p1.objectConcat(p2).rightMap(Some.apply)
          }
        
        case `ArrayConcat` =>
          Arity2(HasPipeline, HasPipeline).flatMap {
            case (p1, p2) => p1.arrayConcat(p2).rightMap(Some.apply)
          }

        case `Filter` => 
          Arity2(HasPipeline, HasSelector).flatMap {
            case (p, q) => addOpSome(p, Match(q))
          }

        case `Drop` =>
          Arity2(HasPipeline, HasInt64).flatMap {
            case (p, v) => addOpSome(p, Skip(v))
          }
        
        case `Take` => 
          Arity2(HasPipeline, HasInt64).flatMap {
            case (p, v) => addOpSome(p, Limit(v))
          }

        case `GroupBy` =>
          Arity2(HasPipeline, HasPipeline).flatMap { 
            case (p1, p2) => p1.groupBy(p2).rightMap(Some.apply)
          }

        case `OrderBy` => 
          args match {
            case _ :: HasSortKeys(keys) :: Nil =>
              Arity2(HasPipeline, HasPipeline).flatMap { 
                case (p1, p2) => p1.sortBy(p2, keys).rightMap(Some.apply)
              }

            case _ => -\/ (FuncApply(func, "array of objects with key and order field", args.toString))
          }

        case `Like`       => \/- (None)

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

        case `Or`         => expr2((a, b) => ExprOp.Or(NonEmptyList.nel(a, b :: Nil))).orElse(\/- (None))
        case `And`        => expr2((a, b) => ExprOp.And(NonEmptyList.nel(a, b :: Nil))).orElse(\/- (None))
        case `Not`        => expr1(ExprOp.Not.apply)

        case `ArrayLength` => 
          Arity2(HasPipeline, HasInt64).flatMap { 
            case (p, v) => // TODO: v should be 1???
              p.expr1(e => \/- (ExprOp.Size(e))).rightMap(Some.apply)
          }

        case `Extract`   => 
          Arity2(HasText, HasPipeline).flatMap {
            case (field, p) =>
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
                case _              => -\/ (FuncApply(func, "valid time period", field))
              }
          }

        case `Between` => expr3((x, l, u) => ExprOp.And(NonEmptyList.nel(ExprOp.Gte(x, l), ExprOp.Lte(x, u) :: Nil)))

        case `ObjectProject` => 
          Arity2(HasPipeline, HasText).flatMap {
            case (p, name) => p.projectField(name).rightMap(Some.apply)
          }

        case `ArrayProject` => 
          Arity2(HasPipeline, HasInt64).flatMap {
            case (p, index) => p.projectIndex(index.toInt).rightMap(Some.apply)
          }

        case `FlattenArray` => Arity1(HasPipeline).flatMap(_.flattenArray).map(Some.apply)

        case `Squash` => Arity1(HasPipeline).flatMap(_.squash).map(Some.apply)

        case _ => -\/ (UnsupportedFunction(func))
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
          join      = (_, _, _, _, _, _) => \/- (None),
          invoke    = invoke(_, _),
          free      = _ =>  \/- (None),
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
