package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._

import collection.immutable.ListMap

import scalaz.{Free => FreeM, Node => _, _}
import Scalaz._

object MongoDbPlanner extends Planner[WorkflowOp] {
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

  type OutputM[A] = Error \/ A

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
  def FieldPhase[A]: Phase[LogicalPlan, A, OutputM[Option[BsonField]]] =
    optimalBoundSynthPara2Phase[A, OutputM[Option[BsonField]]] {
      type Output = OutputM[Option[BsonField]]

      (node: LogicalPlan[(Term[LogicalPlan], Output)]) => {
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
    }

  def JsExprPhase[A]: Phase[LogicalPlan, A, OutputM[Js.Expr => Js.Expr]] =
    optimalBoundSynthPara2Phase[A, OutputM[Js.Expr => Js.Expr]] {
    type Output = OutputM[Js.Expr => Js.Expr]
    type Ann    = (Term[LogicalPlan], Output)

    import PlannerError._

    def convertConstant(src: Data): OutputM[Js.Expr] = src match {
      case Data.Null        => \/-(Js.Null)
      case Data.Str(str)    => \/-(Js.Str(str))
      case Data.True        => \/-(Js.Bool(true))
      case Data.False       => \/-(Js.Bool(false))
      case Data.Dec(num)    => \/-(Js.Num(num.doubleValue, true))
      case Data.Int(num)    => \/-(Js.Num(num.doubleValue, false))
      case Data.Obj(fields) => fields.toList.map(entry => entry match {
        case (k, v) => convertConstant(v).map(const => (k -> const))
      }).sequenceU.map(Js.AnonObjDecl.apply)
      case Data.Arr(values) =>
        values.map(convertConstant).sequenceU.map(Js.AnonElem.apply)
      case Data.Set(values) =>
        values.map(convertConstant).sequenceU.map(Js.AnonElem.apply)
      case _                => -\/(NonRepresentableData(src))
    }

    def invoke(func: Func, args: List[Ann]): Output = {

      val HasJs: Ann => OutputM[Js.Expr => Js.Expr] = _._2
      val HasStr: Ann => OutputM[String] = HasJs(_).flatMap {
        _(Js.Null) match {
          case Js.Str(str) => \/-(str)
          case x =>  -\/(FuncApply(func, "JS string", x.toString))
        }
      }

      def Arity1[A](f: Ann => OutputM[A]): OutputM[A] = args match {
        case a1 :: Nil => f(a1)
        case _         => -\/(FuncArity(func, 1))
      }

      def Arity2[A, B](f1: Ann => OutputM[A], f2: Ann => OutputM[B]):
          OutputM[(A, B)] = args match {
        case a1 :: a2 :: Nil => (f1(a1) |@| f2(a2))((_, _))
        case _               => -\/(FuncArity(func, 2))
      }

      def Arity3[A, B, C](f1: Ann => OutputM[A], f2: Ann => OutputM[B], f3: Ann => OutputM[C]): OutputM[(A, B, C)] = args match {
        case a1 :: a2 :: a3 :: Nil => (f1(a1) |@| f2(a2) |@| f3(a3))((_, _, _))
        case _                     => -\/(FuncArity(func, 3))
      }

      def makeSelect(qualifier: Output, name: String): Output =
        qualifier.map(x => arg => Js.Select(x(arg), name))

      def makeSimpleCall(func: String, args: List[Js.Expr => Js.Expr]):
          Js.Expr => Js.Expr =
        arg => Js.Call(Js.Ident(func), args.map(_(arg)))

      def makeSimpleBinop(op: String, args: List[Ann]): Output =
        Arity2(HasJs, HasJs).map {
          case (lhs, rhs) => arg => Js.BinOp(op, lhs(arg), rhs(arg))
        }

      def makeSimpleUnop(op: String, args: List[Ann]): Output =
        Arity1(HasJs).map(x => arg => Js.UnOp(op, x(arg)))

      func match {
        case `Count` =>
          Arity1(HasJs).map(x => arg => Js.Select(x(arg), "count"))
        case `Length` =>
          Arity1(HasJs).map(x => arg => Js.Select(x(arg), "length"))
        case `Sum` =>
          Arity1(HasJs).map(x => arg =>
            Js.Call(Js.Select(x(arg), "reduce"), List(Js.Ident("+"))))
        case `Min`  =>
          Arity1(HasJs).map {
            case x => arg =>
              Js.Call(
                Js.Select(Js.Select(Js.Ident("Math"), "min"), "apply"),
                List(Js.Null, x(arg)))
          }
        case `Max`  =>
          Arity1(HasJs).map {
            case x => arg =>
              Js.Call(
                Js.Select(Js.Select(Js.Ident("Math"), "max"), "apply"),
                List(Js.Null, x(arg)))
          }
        case `Eq`   => makeSimpleBinop("==", args)
        case `Neq`  => makeSimpleBinop("!=", args)
        case `Lt`   => makeSimpleBinop("<",  args)
        case `Lte`  => makeSimpleBinop("<=", args)
        case `Gt`   => makeSimpleBinop(">",  args)
        case `Gte`  => makeSimpleBinop(">=", args)
        case `And`  => makeSimpleBinop("&&", args)
        case `Or`   => makeSimpleBinop("||", args)
        case `Not`  => makeSimpleUnop("!", args)
        case `In`   =>
          Arity2(HasJs, HasJs).map {
            case (value, array) => arg =>
              Js.BinOp("!=",
                Js.Num(-1, false),
                Js.Call(Js.Select(array(arg), "indexOf"), List(value(arg))))
          }
        case `Search` =>
          Arity2(HasJs, HasJs).map { case (field, pattern) =>
            x => Js.Call(Js.Select(field(x), "match"), List(pattern(x)))
          }
        case `Extract` =>
          Arity2(HasStr, HasJs).flatMap { case (field, source) =>
            field match {
              case "century"      => \/- (x => Js.BinOp("/", Js.Call(Js.Select(source(x), "getFullYear"), Nil), Js.Num(100, false)))
              case "day"          => \/- (x => Js.Call(Js.Select(source(x), "getDate"), Nil))  // (day of month)
              case "decade"       => \/- (x => Js.BinOp("/", Js.Call(Js.Select(source(x), "getFullYear"), Nil), Js.Num(10, false)))
              // Note: MongoDB's Date's getDay (during filtering at least) seems to be monday=0 ... sunday=6,
              // apparently in violation of the JavaScript convention.
              case "dow"          => \/- (x => Js.Ternary(Js.BinOp("==", 
                                                          Js.Call(Js.Select(source(x), "getDay"), Nil),
                                                          Js.Num(6, false)),
                                                    Js.Num(0, false),
                                                    Js.BinOp("+", 
                                                      Js.Call(Js.Select(source(x), "getDay"), Nil),
                                                      Js.Num(1, false))))
              // TODO: case "doy"          => \/- (???)
              // TODO: epoch
              case "hour"         => \/- (x => Js.Call(Js.Select(source(x), "getHours"), Nil))
              case "isodow"       => \/- (x => Js.BinOp("+",
                                                  Js.Call(Js.Select(source(x), "getDay"), Nil),
                                                  Js.Num(1, false)))
              // TODO: isoyear
              case "microseconds" => \/- (x => Js.BinOp("*",
                                            Js.BinOp("+", 
                                              Js.Call(Js.Select(source(x), "getMilliseconds"), Nil),
                                              Js.BinOp("*", Js.Call(Js.Select(source(x), "getSeconds"), Nil), Js.Num(1000, false))),
                                            Js.Num(1000, false)))
              case "millennium"   => \/- (x => Js.BinOp("/", Js.Call(Js.Select(source(x), "getFullYear"), Nil), Js.Num(1000, false)))
              case "milliseconds" => \/- (x => Js.BinOp("+", 
                                            Js.Call(Js.Select(source(x), "getMilliseconds"), Nil),
                                            Js.BinOp("*", Js.Call(Js.Select(source(x), "getSeconds"), Nil), Js.Num(1000, false))))
              case "minute"       => \/- (x => Js.Call(Js.Select(source(x), "getMinutes"), Nil))
              case "month"        => \/- (x => Js.BinOp("+", 
                                                Js.Call(Js.Select(source(x), "getMonth"), Nil),
                                                Js.Num(1, false)))
              case "quarter"      => \/- (x => Js.BinOp("+",
                                                Js.BinOp("|",
                                                  Js.BinOp("/",
                                                    Js.Call(Js.Select(source(x), "getMonth"), Nil),
                                                    Js.Num(3, false)),
                                                  Js.Num(0, false)),
                                                Js.Num(1, false)))
              case "second"       => \/- (x => Js.Call(Js.Select(source(x), "getSeconds"), Nil))
              // TODO: timezone, timezone_hour, timezone_minute
              // case "week"         => \/- (???)
              case "year"         => \/- (x => Js.Call(Js.Select(source(x), "getFullYear"), Nil))
              
              case _ => -\/ (FuncApply(func, "valid time period", field))
            }
          }
        case `Between` =>
          Arity3(HasJs, HasJs, HasJs).map {
            case (value, min, max) =>
              makeSimpleCall(
                "&&",
                List(
                  makeSimpleCall("<=", List(min, value)),
                  makeSimpleCall("<=", List(value, max))))
          }
        case `ObjectProject` =>
          Arity2(HasJs, HasStr).map {
            case (x, y) => arg => Js.Select(x(arg), y)
          }
        case `ArrayProject` =>
          Arity2(HasJs, HasJs).map {
            case (x, y) => arg => Js.Access(x(arg), y(arg))
          }
        
        case `Cross` => \/- (identity)
        
        case _ => -\/(UnsupportedFunction(func))
      }
    }

    (node: LogicalPlan[(Term[LogicalPlan], Output)]) => {
      node.fold[Output](
        read      = Function.const(\/-(identity)),
        constant  = const => convertConstant(const).map(Function.const(_)),
        join      = (left, right, tpe, rel, lproj, rproj) =>
          -\/(UnsupportedPlan(node)),
        invoke    = invoke(_, _),
        
        free      = _         => sys.error("never reached: boundPhase handles these nodes"),
        let       = (_, _, _) => sys.error("never reached: boundPhase handles these nodes"))
    }
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
  def SelectorPhase:
      Phase[
        LogicalPlan,
        (OutputM[Option[BsonField]], OutputM[Js.Expr => Js.Expr]),
        OutputM[Selector]] =
    lpBoundPhase {
      type Input = (OutputM[Option[BsonField]], OutputM[Js.Expr => Js.Expr])
      type Output = OutputM[Selector]

      Phase { (attr: Attr[LogicalPlan,Input]) =>
      scanPara2(attr) { (fieldAttr: Input, node: LogicalPlan[(Term[LogicalPlan], Input, Output)]) =>
        def invoke(func: Func, args: List[(Term[LogicalPlan], Input, Output)]): Output = {
          object IsBson {
            def unapply(v: Term[LogicalPlan]): Option[Bson] = v match {
              case Constant(b) => Bson.fromData(b).toOption
              
              case Invoke(`Negate`, Constant(Data.Int(i)) :: Nil) => Some(Bson.Int64(-i.toLong))
              case Invoke(`Negate`, Constant(Data.Dec(x)) :: Nil) => Some(Bson.Dec(-x.toDouble))
              
              case _ => None
            }
          }

          /**
           * Attempts to extract a BsonField annotation and a Bson value from
           * an argument list of length two (in any order).
           */
            def extractFieldAndSelector: OutputM[(BsonField, Bson)] = args match {
              case (IsBson(v1), _, _) :: (_, (\/-(Some(f2)), _), _) :: Nil => \/-(f2 -> v1)
              case (_, (\/-(Some(f1)), _), _) :: (IsBson(v2), _, _) :: Nil => \/-(f1 -> v2)
              case _ => -\/(PlannerError.UnsupportedPlan(node))
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
                x <- extractFieldAndSelector
                (field, value) = x
            } yield Selector.Doc(ListMap(field -> Selector.Expr(f(value))))

          def stringOp(f: String => Selector.Condition) =
            for {
                x <- extractFieldAndSelector
                (field, value) = x
                str <- value match {
                  case Bson.Text(s) => \/-(s)
                  case _ => -\/(PlannerError.UnsupportedPlan(node))
                }
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

            case `Search`   => stringOp(s => Selector.Regex(s, false, false, false, false))

            case `Between`  => args match {
                case (_, (\/-(Some(f)), _), _) :: (IsBson(lower), _, _) :: (IsBson(upper), _, _) :: Nil =>
                  \/-(Selector.And(
                  Selector.Doc(f -> Selector.Gte(lower)),
                  Selector.Doc(f -> Selector.Lte(upper))
                ))

                case _ => -\/(PlannerError.UnsupportedPlan(node))
            }

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            // case `Not`      => invoke1(Selector.Not.apply _)

              case _ => -\/(PlannerError.UnsupportedFunction(func))
          }
        }

        node.fold[Output](
          read     = _ => -\/(PlannerError.UnsupportedPlan(node)),
          constant = _ => -\/(PlannerError.UnsupportedPlan(node)),
          join     = (_, _, _, _, _, _) =>
            -\/(PlannerError.UnsupportedPlan(node)),
          invoke   = (f, vs) =>
            invoke(f,vs) <+> fieldAttr._2.map(js => Selector.Where(js(Js.This))),
          free     = _ => -\/(PlannerError.UnsupportedPlan(node)),
          let      = (_, _, in) => in._3)
      }
    }
  }

  def WorkflowPhase: PhaseS[
    LogicalPlan,
    NameGen,
    (OutputM[Selector], OutputM[Js.Expr => Js.Expr]),
    OutputM[WorkflowBuilder]] = optimalBoundPhaseS {
      
    import WorkflowBuilder._

    type Input  = (OutputM[Selector], OutputM[Js.Expr => Js.Expr])

    type Output = M[WorkflowBuilder]
    
    type Ann    = Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]

    import LogicalPlan._
    import LogicalPlan.JoinType._
    import PipelineOp._
    import Js._
    import WorkflowOp._
    import PlannerError._

    object HasData {
      def unapply(node: Ann): Option[Data] = node match {
        case LogicalPlan.Constant.Attr(data) => Some(data)
        case _ => None
      }
    }

    object IsSortKey {
      def unapply(node: Ann): Option[SortType] = 
        node match {
          case HasData(Data.Str("ASC"))  => Some(Ascending)
          case HasData(Data.Str("DESC")) => Some(Descending)
          case _ => None
        }
    }

    object HasSortKeys {
      def unapply(v: Ann): Option[List[SortType]] = {
        v match {
          case MakeArrayN.Attr(array) =>
            array.map(IsSortKey.unapply(_)).sequenceU
          case _ => None
        }
      }
    }

    val HasSelector: Ann => M[Selector] = ann => lift(ann.unFix.attr._1._1)

    val HasJs: Ann => M[Js.Expr => Js.Expr] = ann => lift(ann.unFix.attr._1._2)

    val HasWorkflow: Ann => M[WorkflowBuilder] = ann => lift(ann.unFix.attr._2)
    
    val HasAny: Ann => M[Unit] = _ => emit(())
    
    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]]): Output = {

      val HasLiteral: Ann => M[Bson] = ann => HasWorkflow(ann).flatMap { p =>
        p.asLiteral match {
          case Some(ExprOp.Literal(value)) => emit(value)
          case _ => fail(FuncApply(func, "literal", p.toString))
        }
      }

      val HasInt64: Ann => M[Long] = HasLiteral(_).flatMap {
        case Bson.Int64(v) => emit(v)
        case x => fail(FuncApply(func, "64-bit integer", x.toString))
      }

      val HasText: Ann => M[String] = HasLiteral(_).flatMap {
        case Bson.Text(v) => emit(v)
        case x => fail(FuncApply(func, "text", x.toString))
      }

      def Arity1[A](f: Ann => M[A]): M[A] = args match {
        case a1 :: Nil => f(a1)
        case _ => fail(FuncArity(func, 1))
      }

      def Arity2[A, B](f1: Ann => M[A], f2: Ann => M[B]): M[(A, B)] = args match {
        case a1 :: a2 :: Nil => (f1(a1) |@| f2(a2))((_, _))
        case _ => fail(FuncArity(func, 2))
      }

      def Arity3[A, B, C](f1: Ann => M[A], f2: Ann => M[B], f3: Ann => M[C]): M[(A, B, C)] = args match {
        case a1 :: a2 :: a3 :: Nil => (f1(a1) |@| f2(a2) |@| f3(a3))((_, _, _))
        case _ => fail(FuncArity(func, 3))
      }

      def expr1(f: ExprOp => ExprOp): Output =
        Arity1(HasWorkflow).flatMap(_.expr1(x => \/- (f(x))))

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output =
        Arity1(HasWorkflow).flatMap(wf => wf.reduce(f))

      def mapExpr(p: WorkflowBuilder)(f: ExprOp => ExprOp): Output =
        p.expr1(e => \/- (f(e)))

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output =
        Arity2(HasWorkflow, HasWorkflow).flatMap {
          case (p1, p2) => p1.expr2(p2) { (l, r) => \/-(f(l, r)) }
        }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output =
        Arity3(HasWorkflow, HasWorkflow, HasWorkflow).flatMap {
          case (p1, p2, p3) => p1.expr3(p2, p3)((a, b, c) => \/-(f(a, b, c)))
        }

      func match {
        case `MakeArray` =>
          Arity1(HasWorkflow).map(_.makeArray)
        case `MakeObject` =>
          Arity2(HasText, HasWorkflow).map {
            case (name, wf) => wf.makeObject(name)
          }
        case `ObjectConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => p1.objectConcat(p2)
          }
        case `ArrayConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => p1.arrayConcat(p2)
          }
        case `Filter` =>
          Arity2(HasWorkflow, HasSelector).map {
            case (p, q) => p >>> matchOp(q)
          }
        case `Drop` =>
          Arity2(HasWorkflow, HasInt64).map {
            case (p, v) => p >>> skipOp(v)
          }
        case `Take` =>
          Arity2(HasWorkflow, HasInt64).map {
            case (p, v) => p >>> limitOp(v)
          }
        case `Cross` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (l, r) => lift(l.cross(r))
          }
        case `GroupBy` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => p1.groupBy(p2)
          }
        case `OrderBy` =>
          args match {
            case _ :: _ :: HasSortKeys(keys) :: Nil =>
              Arity3(HasWorkflow, HasWorkflow, HasAny).flatMap {
                case (p1, p2, _) => p1.sortBy(p2, keys)
              }

            case _ => fail(FuncApply(func, "array of objects with key and order field", args.toString))
          }

        case `Add`        => expr2(ExprOp.Add.apply _)
        case `Multiply`   => expr2(ExprOp.Multiply.apply _)
        case `Subtract`   => expr2(ExprOp.Subtract.apply _)
        case `Divide`     => expr2(ExprOp.Divide.apply _)
        case `Modulo`     => expr2(ExprOp.Mod.apply _)
        case `Negate`     => expr1(ExprOp.Multiply(ExprOp.Literal(Bson.Int32(-1)), _))

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

        case `Or`         => expr2((a, b) => ExprOp.Or(NonEmptyList.nel(a, b :: Nil)))
        case `And`        => expr2((a, b) => ExprOp.And(NonEmptyList.nel(a, b :: Nil)))
        case `Not`        => expr1(ExprOp.Not.apply)

        case `ArrayLength` =>
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, v) => // TODO: v should be 1???
              p.expr1(e => \/- (ExprOp.Size(e)))
          }

        case `Extract`   =>
          Arity2(HasText, HasWorkflow).flatMap {
            case (field, p) =>
              field match {
                case "century"      =>
                  mapExpr(p) { v =>
                    ExprOp.Divide(
                      ExprOp.Year(v),
                      ExprOp.Literal(Bson.Int32(100)))
                  }
                case "day"          => mapExpr(p)(ExprOp.DayOfMonth(_))
                case "decade"       => mapExpr(p)(x => ExprOp.Divide(ExprOp.Year(x), ExprOp.Literal(Bson.Int64(10))))
                case "dow"          => mapExpr(p)(x => ExprOp.Add(ExprOp.DayOfWeek(x), ExprOp.Literal(Bson.Int64(-1))))
                case "doy"          => mapExpr(p)(ExprOp.DayOfYear(_))
                // TODO: epoch
                case "hour"         => mapExpr(p)(ExprOp.Hour(_))
                case "isodow"       => mapExpr(p)(x => ExprOp.Cond(
                  ExprOp.Eq(ExprOp.DayOfWeek(x), ExprOp.Literal(Bson.Int64(1))),
                  ExprOp.Literal(Bson.Int64(7)),
                  ExprOp.Add(ExprOp.DayOfWeek(x), ExprOp.Literal(Bson.Int64(-1)))))
                // TODO: isoyear
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
                case "quarter"      => // TODO: handle leap years
                  mapExpr(p) { v =>
                    ExprOp.Add(
                      ExprOp.Divide(
                        ExprOp.DayOfYear(v),
                        ExprOp.Literal(Bson.Int32(92))),
                      ExprOp.Literal(Bson.Int32(1)))
                  }
                case "second"       => mapExpr(p)(ExprOp.Second(_))
                // TODO: timezone, timezone_hour, timezone_minute
                case "week"         => mapExpr(p)(ExprOp.Week(_))
                case "year"         => mapExpr(p)(ExprOp.Year(_))
                case _              => fail(FuncApply(func, "valid time period", field))
              }
          }

        case `Between` => expr3((x, l, u) => ExprOp.And(NonEmptyList.nel(ExprOp.Gte(x, l), ExprOp.Lte(x, u) :: Nil)))

        case `ObjectProject` =>
          Arity2(HasWorkflow, HasText).map {
            case (p, name) => p.projectField(name)
          }
        case `ArrayProject` =>
          Arity2(HasWorkflow, HasInt64).map {
            case (p, index) => p.projectIndex(index.toInt)
          }
        case `FlattenObject` => Arity1(HasWorkflow).map(_.flattenObject)
        case `FlattenArray` => Arity1(HasWorkflow).map(_.flattenArray)
        case `Squash`       => Arity1(HasWorkflow).map(_.squash)
        case `Distinct`     => Arity1(HasWorkflow).flatMap(p => p.distinctBy(p))
        case `DistinctBy`   => Arity2(HasWorkflow, HasWorkflow).flatMap { case (p, key) => p.distinctBy(key) }

        case _ => fail(UnsupportedFunction(func))
      }
    }

    // Tricky: It's easier to implement each step using StateT[\/, ...], but we need the phase's
    // Monad to be State[..., \/], so that the morphism flatMaps over the State but not the \/.
    // That way it can evaluate to left for an individual node without failing the phase.
    // This code takes care of mapping from one to the other.
    (node: LogicalPlan[Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]]) =>
      node.fold[State[NameGen, Error \/ WorkflowBuilder]](
        read      = path =>
          state(Collection.fromPath(path).map(WorkflowBuilder.read)),

        constant  = data =>
          state(Bson.fromData(data).bimap(
            _ => PlannerError.NonRepresentableData(data),
            x => WorkflowBuilder.pure(x))),

        join      = (left, right, tpe, comp, leftKey, rightKey) => {
          def js(attr: Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]): OutputM[Js.Expr => Js.Expr] = attr.unFix.attr._1._2
          def workflow(attr: Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]): Error \/ WorkflowBuilder = attr.unFix.attr._2
        
          state(for {
            l  <- workflow(left)
            r  <- workflow(right)
            lk <- workflow(leftKey).flatMap(x => x.asExprOp \/> InternalError("Can’t represent " + x + "as Expr."))
            rk <- js(rightKey)
            rez <- l.join(r, tpe, comp, lk, rk)
          } yield rez)
        },

        invoke    = (func, args) => {
          val v = invoke(func, args)
          State(s => v.run(s).fold(e => s -> -\/ (e), t => t._1 -> \/- (t._2)))
        },

        free      = _         => sys.error("never reached: boundPhase handles these nodes"),
        let       = (_, _, _) => sys.error("never reached: boundPhase handles these nodes"))
  }

  // FieldPhase   JsExprPhase
  //           \ /          |
  //            |          /
  //      SelectorPhase   /
  //                   \ /
  //                    |
  //              WorkflowPhase
  val AllPhases: PhaseS[LogicalPlan, NameGen, Unit, Error \/ WorkflowBuilder] =
    liftPhaseS((FieldPhase[Unit] &&& JsExprPhase[Unit])
      .fork(SelectorPhase, PhaseMArrow[Id, LogicalPlan].arr(_._2))) >>>
      WorkflowPhase

  def plan(logical: Term[LogicalPlan]): OutputM[WorkflowOp] = {
    val a: State[NameGen, Attr[LogicalPlan, Error \/ WorkflowBuilder]] = AllPhases(attrUnit(logical))
    a.evalZero.unFix.attr.map(_.build)
  }
}
