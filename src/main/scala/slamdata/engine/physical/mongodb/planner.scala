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

  // NB: This is used for both Selector and JsExpr phases, but may need to be
  //     split if their regex dialects aren't close enough for our purposes.
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

  def JsExprPhase[A]: PhaseE[LogicalPlan, Error, A, Option[Js.Expr]] = {
    type Output = Option[Js.Expr]

    def convertConstant(src: Data): Option[Js.Expr] = src match {
      case Data.Null        => Some(Js.Null)
      case Data.Str(str)    => Some(Js.Str(str))
      case Data.True        => Some(Js.Bool(true))
      case Data.False       => Some(Js.Bool(false))
      case Data.Dec(num)    => Some(Js.Num(num.doubleValue, true))
      case Data.Int(num)    => Some(Js.Num(num.doubleValue, false))
      case Data.Obj(fields) => fields.toList.map(entry => entry match {
        case (k, v) => convertConstant(v).map(const => (k -> const))
      }).sequence.map(Js.AnonObjDecl.apply)
      case Data.Arr(values) =>
        values.map(convertConstant).sequence.map(Js.AnonElem.apply)        
      case Data.Set(values) =>
        values.map(convertConstant).sequence.map(Js.AnonElem.apply)
      case _ => None
    }

    def invoke(func: Func, args: List[Option[Js.Expr]]):
        Option[Js.Expr] = {
      type Output = Option[Js.Expr]

      def makeSelect(qualifier: Output, name: String): Output =
        qualifier.map(Js.Select(_, name))

      def makeAccess(qualifier: Output, key: Output): Output =
        Apply[Option].lift2(Js.Access)(qualifier, key)

      def makeSimpleCall(func: String, args: List[Output]): Output =
        args.sequence.map(Js.Call(Js.Ident(func), _))

      def makeSimpleBinop(op: String, args: List[Output]): Output = {
        val lhs :: rhs :: Nil = args
        for {
          lhs <- lhs
          rhs <- rhs
        } yield Js.BinOp(op, lhs, rhs)
      }

      def makeSimpleUnop(op: String, args: List[Output]): Output = {
        val operand :: Nil = args
        for {
          operand <- operand
        } yield Js.UnOp(op, operand)
      }

      func match {
        case `Count` =>
          val qualifier :: Nil = args
          makeSelect(qualifier, "count")
        case `Length` =>
          val qualifier :: Nil = args
          makeSelect(qualifier, "length")
        case `Sum` =>
          val qualifier :: Nil = args
          makeSelect(qualifier, "reduce").map(Js.Call(_, List(Js.Ident("+"))))
        case `Min`  =>
          args(0).map { arg =>
            Js.Call(
              Js.Select(Js.Select(Js.Ident("Math"), "min"), "apply"),
              List(Js.Null, arg))}
        case `Max`  =>
          args(0).map { arg =>
            Js.Call(
              Js.Select(Js.Select(Js.Ident("Math"), "max"), "apply"),
              List(Js.Null, arg))}
        case `Eq`   => makeSimpleBinop("==", args)
        case `Neq`  => makeSimpleBinop("!=", args)
        case `Lt`   => makeSimpleBinop("<",  args)
        case `Lte`  => makeSimpleBinop("<=", args)
        case `Gt`   => makeSimpleBinop(">",  args)
        case `Gte`  => makeSimpleBinop(">=", args)
        case `And`  => makeSimpleBinop("&&", args)
        case `Or`   => makeSimpleBinop("||", args)
        case `Not`  => makeSimpleBinop("!", args)
        // case `Like` =>
        //   args(0).flatMap { arg =>
        //     makeCall("match", Js.Str(regexForLikePattern(arg)))
        //   }
        case `Between` => {
          val value :: min :: max :: Nil = args
          makeSimpleCall(
            "&&",
            List(
              makeSimpleCall("<=", List(min, value)),
              makeSimpleCall("<=", List(value, max))))
        }
        case `ObjectProject` => args match {
          case qualifier :: Some(Js.Str(key)) :: Nil =>
            makeSelect(qualifier, key)
          case _ => None
        }
        case `ArrayProject` =>
          val qualifier :: key :: Nil = args
          makeAccess(qualifier, key)
        case x => None
      }
    }

    liftPhaseE(Phase { (attr: Attr[LogicalPlan, A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(Term[LogicalPlan], Output)]) =>
        node.fold[Output](
          read      = Function.const(Some(Js.Ident("this"))),
          constant  = const => convertConstant(const),
          join      = (left, right, tpe, rel, lproj, rproj) => None,
          invoke    = (func, args) => invoke(func, args.map(_._2)),
          free      = Function.const(Some(Js.Ident("this"))),
          let       = (ident, form, body) => for {
            b <- body._2
            f <- form._2
          } yield Js.Call(Js.AnonFunDecl(List(ident.name), List(b)), List(f))
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
  def SelectorPhase:
      PhaseE[
        LogicalPlan,
        Error,
        (Option[BsonField], Option[Js.Expr]),
        Option[Selector]] =
    lpBoundPhaseE {
    type Input = (Option[BsonField], Option[Js.Expr])
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
            case (IsBson(v1), _, _) :: (_, (Some(f2), _), _) :: Nil => Some(f2 -> v1)
            case (_, (Some(f1), _), _) :: (IsBson(v2), _, _) :: Nil => Some(f1 -> v2)
            case _                                                  => None
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

          func match {
            case `Eq`       => relop(Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _)

            case `Like`     => stringOp(s => Selector.Regex(regexForLikePattern(s), false, false, false, false))

            case `Between`  => args match {
              case (_, (Some(f), _), _) :: (IsBson(lower), _, _) :: (IsBson(upper), _, _) :: Nil =>
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
          invoke    = (f, vs) => invoke(f, vs) <+> fieldAttr._2.map(Selector.Where.apply _),
          free      = _ => None,
          let       = (_, _, in) => in._3
        )
      }
    })
  }

  def WorkflowPhase: PhaseE[
    LogicalPlan,
    Error,
    (Option[Selector], Option[Js.Expr]),
    Option[WorkflowBuilder]] = lpBoundPhaseE {
    type Input  = (Option[Selector], Option[Js.Expr])
    type Output = Error \/ Option[WorkflowBuilder]
    type OutputM[A] = Error \/ A
    type Ann    = Attr[LogicalPlan, (Input, Output)]


    import LogicalPlan._
    import LogicalPlan.JoinType._
    import PipelineOp._
    import Js._
    import WorkflowOp._
    import PlannerError._

    def nothing = \/-(None)

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

    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Output)]]): Output = {

      val HasSelector: Ann => Error \/ Selector = {
        case Attr(((Some(sel), _), _), _) => \/- (sel)
        case _ => -\/ (FuncApply(func, "selector", "none"))
      }

      val HasJs: Ann => Error \/ Js.Expr = {
        case Attr(((_, Some(js)), _), _) => \/- (js)
        case _ => -\/ (FuncApply(func, "JavaScript", "none"))
      }

      val HasWorkflow: Ann => Error \/ WorkflowBuilder = 
        _.unFix.attr._2.toOption.flatten.map(\/- apply).getOrElse(-\/ (FuncApply(func, "workflow", "nothing")))

      val HasLiteral: Ann => Error \/ Bson = HasWorkflow(_).flatMap { p =>
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

      def expr1(f: ExprOp => ExprOp): Output = Arity1(HasWorkflow).flatMap {
        _.expr1(e => \/- (f(e))).rightMap(Some.apply)
      }

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output = Arity1(HasWorkflow).flatMap { p =>    
        (for {
          p <- if (p.isGrouped) \/- (p) else p.groupBy(WorkflowBuilder.fromExpr(DummyOp, ExprOp.Literal(Bson.Int32(1))))
          p <- p.reduce(f)
        } yield p).rightMap(Some.apply)
      }

      def mapExpr(p: WorkflowBuilder)(f: ExprOp => ExprOp): Output = {
        p.expr1(e => \/- (f(e))).rightMap(Some.apply)
      }

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output = Arity2(HasWorkflow, HasWorkflow).flatMap {
        case (p1, p2) =>
          p1.expr2(p2) { (l, r) => \/- (f(l, r)) }.rightMap(Some.apply)
      }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output = Arity3(HasWorkflow, HasWorkflow, HasWorkflow).flatMap { 
        case (p1, p2, p3) => p1.expr3(p2, p3)((a, b, c) => \/- (f(a, b, c))).rightMap(Some.apply)
      }

      func match {
        case `MakeArray` =>
          Arity1(HasWorkflow).flatMap(_.makeArray.rightMap(Some.apply))
        case `MakeObject` =>
          Arity2(HasText, HasWorkflow).flatMap {
            case (name, wf) => wf.makeObject(name).rightMap(Some.apply)
          }
        case `ObjectConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => p1.objectConcat(p2).rightMap(Some.apply)
          }
        case `ArrayConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => p1.arrayConcat(p2).rightMap(Some.apply)
          }
        case `Filter` =>
          Arity2(HasWorkflow, HasSelector).flatMap {
            case (p, q) => (p &&& (MatchOp(_, q))).rightMap(Some.apply)
          }
        case `Drop` =>
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, v) => (p >>> (SkipOp(_, v))).rightMap(Some.apply)
          }
        case `Take` => 
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, v) => (p >>> (LimitOp(_, v))).rightMap(Some.apply)
          }
        case `GroupBy` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap { 
            case (p1, p2) => p1.groupBy(p2).rightMap(Some.apply)
          }
        case `OrderBy` =>
          args match {
            case _ :: HasSortKeys(keys) :: Nil =>
              Arity2(HasWorkflow, HasWorkflow).flatMap { 
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
          Arity2(HasWorkflow, HasInt64).flatMap { 
            case (p, v) => // TODO: v should be 1???
              p.expr1(e => \/- (ExprOp.Size(e))).rightMap(Some.apply)
          }

        // case `Length`      => expr1(ExprOp.Length.apply _)

        case `Extract`   => 
          Arity2(HasText, HasWorkflow).flatMap {
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
          Arity2(HasWorkflow, HasText).flatMap {
            case (p, name) => p.projectField(name).rightMap(Some.apply)
          }
        case `ArrayProject` =>
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, index) => p.projectIndex(index.toInt).rightMap(Some.apply)
          }
        case `FlattenArray` => Arity1(HasWorkflow).flatMap(_.flattenArray).map(Some.apply)
        case `Squash` => Arity1(HasWorkflow).flatMap(_.squash).map(Some.apply)
        case _ => -\/ (UnsupportedFunction(func))
      }
    }

    toPhaseE(Phase[LogicalPlan, Input, Output] { (attr: Attr[LogicalPlan, Input]) =>
      scanPara0(attr) {
        (orig: Attr[LogicalPlan, Input],
         node: LogicalPlan[Attr[LogicalPlan, (Input, Output)]]) =>
        val (optSel, optJs) = orig.unFix.attr

        node.fold[Output](
          read      = path => \/-(Some(WorkflowBuilder.read(path))),
          constant  = data => Bson.fromData(data).bimap(
            Function.const(PlannerError.NonRepresentableData(data)),
            x => Some(WorkflowBuilder.pure(x))),
          join      = (left, right, tpe, comp, leftKey, rightKey) => {
            left.unFix.attr._2.flatMap { l =>
              right.unFix.attr._2.flatMap { r =>
                (for {
                  lk <- leftKey.unFix.attr._2.toOption.join.flatMap(_.asExprOp)
                  rk <- rightKey.unFix.attr._1._2
                  l0 <- l
                  r0 <- r
                } yield WorkflowBuilder.join(l0, r0, tpe, lk, rk)
                ) match {
                  case None =>
                    -\/(PlannerError.UnsupportedPlan(orig.unFix.unAnn))
                  case wf => \/-(wf)
                }
              }
            }},
          invoke    = invoke(_, _),
          free      = _ =>  \/- (None),
          let       = (_, _, in) => in.unFix.attr._2
        )
      }
    })
  }

  // FieldPhase   JsExprPhase
  //           \ /          |
  //            |          /
  //      SelectorPhase   /
  //                   \ /
  //                    |
  //              WorkflowPhase
  val AllPhases =
    (FieldPhase[Unit] &&& JsExprPhase[Unit])
      .fork(
        SelectorPhase,
        liftPhaseE(PhaseMArrow[Id, LogicalPlan].arr(_._2))) >>>
      WorkflowPhase

  def plan(logical: Term[LogicalPlan]): Error \/ Workflow =
    AllPhases(attrUnit(logical)).flatMap(x => x.unFix.attr match {
      case None => -\/(PlannerError.UnsupportedPlan(logical.unFix))
      case Some(wf) => wf.build
    })
}
