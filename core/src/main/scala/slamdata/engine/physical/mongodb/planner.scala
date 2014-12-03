package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._
import slamdata.engine.javascript._
import Workflow._

import collection.immutable.ListMap

import scalaz.{Free => FreeM, Node => _, _}
import Scalaz._

import org.threeten.bp.{Duration, Instant}

object MongoDbPlanner extends Planner[Workflow] {
  import LogicalPlan._
  import WorkflowBuilder._

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

  def parseTimestamp(str: String): Error \/ Bson.Date =
    \/.fromTryCatchNonFatal(Instant.parse(str)).bimap(
      _    => PlannerError.DateFormatError(ToTimestamp, str),
      inst => Bson.Date(inst))

  def parseInterval(str: String): Error \/ Bson.Dec =
    \/.fromTryCatchNonFatal(Duration.parse(str)).bimap(
      _   => PlannerError.DateFormatError(ToInterval, str),
      dur => Bson.Dec(dur.getSeconds*1000 + dur.getNano*1e-6))

  // TODO: switch this phase to JsCore, so that these annotations can be
  // used by the workflow phase, instead of duplicating some of the 
  // code there. Will probably need to adopt the approach taken by 
  // the SelectorPhase, where actual selector construction is delayed 
  // until the workflowphase has the workflowbuilder for each data
  // source. See #477.
  def JsExprPhase[A]:
    Phase[LogicalPlan, A, OutputM[Js.Expr => Js.Expr]] = {
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
        case `Add`      => makeSimpleBinop("+", args)
        case `Multiply` => makeSimpleBinop("*", args)
        case `Subtract` => makeSimpleBinop("-", args)
        case `Divide`   => makeSimpleBinop("/", args)
        case `Modulo`   => makeSimpleBinop("%", args)
        case `Negate`   => makeSimpleUnop("-", args)

        case `Eq`  => makeSimpleBinop("==", args)
        case `Neq` => makeSimpleBinop("!=", args)
        case `Lt`  => makeSimpleBinop("<",  args)
        case `Lte` => makeSimpleBinop("<=", args)
        case `Gt`  => makeSimpleBinop(">",  args)
        case `Gte` => makeSimpleBinop(">=", args)
        case `And` => makeSimpleBinop("&&", args)
        case `Or`  => makeSimpleBinop("||", args)
        case `Not` => makeSimpleUnop("!", args)
        case `IsNull` => Arity1(HasJs).map(x => arg => Js.BinOp("==", x(arg), Js.Null))
        case `In`  =>
          Arity2(HasJs, HasJs).map {
            case (value, array) => arg =>
              Js.BinOp("!=",
                Js.Num(-1, false),
                Js.Call(Js.Select(array(arg), "indexOf"), List(value(arg))))
          }
        case `Search` =>
          Arity2(HasJs, HasJs).map { case (field, pattern) =>
            x => Js.Call(Js.Select(Js.New(Js.Call(Js.Ident("RegExp"), List(pattern(x)))), "test"), List(field(x)))
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
        case `ToTimestamp` => for {
          str  <- Arity1(HasStr)
          date <- parseTimestamp(str)
        } yield (_ => Js.New(Js.Call(Js.Ident("Date"), List(Js.Str(str)))))
        case `ToInterval` => for {
          str <- Arity1(HasStr)
          dur <- parseInterval(str)
        } yield (_ => Js.Num(dur.value, true))
          
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
        
        case _ => -\/(UnsupportedFunction(func))
      }
    }

    Phase { (attr: Attr[LogicalPlan, A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[Ann]) =>
        node.fold[Output](
          read      = Function.const(\/-(identity)),
          constant  = const => convertConstant(const).map(Function.const(_)),
          join      = (_, _, _, _, _, _) => -\/(UnsupportedPlan(node)),
          invoke    = invoke(_, _),
          free      = Function.const(\/-(identity)),
          let       = (ident, form, body) =>
            (body._2 |@| form._2)((b, f) =>
              arg => Js.Let(Map(ident.name -> f(arg)), Nil, b(arg))))
      }
    }
  }

  type InputFinder[A] = Attr[LogicalPlan, A] => A
  def here[A]: InputFinder[A] = _.unFix.attr
  def there[A](index: Int, next: InputFinder[A]): InputFinder[A] =
    a => next((a.children.apply)(index))
  type PartialSelector[A] =
    (PartialFunction[List[BsonField], Selector], List[InputFinder[A]])

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
  def SelectorPhase[A, B]: Phase[LogicalPlan, A, OutputM[PartialSelector[B]]] =
    lpBoundPhase {
      type Output = OutputM[PartialSelector[B]]

      object IsBson {
        def unapply(v: (Term[LogicalPlan], A, Output)): Option[Bson] = v match {
          case (Constant(b), _, _) => Bson.fromData(b).toOption
          
          case (Invoke(`Negate`, Constant(Data.Int(i)) :: Nil), _, _) => Some(Bson.Int64(-i.toLong))
          case (Invoke(`Negate`, Constant(Data.Dec(x)) :: Nil), _, _) => Some(Bson.Dec(-x.toDouble))
          
          case (Invoke(`ToTimestamp`, Constant(Data.Str(str)) :: Nil), _, _) => parseTimestamp(str).toOption
          case (Invoke(`ToInterval`, Constant(Data.Str(str)) :: Nil), _, _) => parseInterval(str).toOption
          
          case _ => None
        }
      }
      
      object IsText {
        def unapply(v: (Term[LogicalPlan], A, Output)): Option[String] = v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _ => None
        }
      }

      Phase { (attr: Attr[LogicalPlan, A]) =>
      scanPara2(attr) { (fieldAttr: A, node: LogicalPlan[(Term[LogicalPlan], A, Output)]) =>
        def invoke(func: Func, args: List[(Term[LogicalPlan], A, Output)]): Output = {
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
          def relop(f: Bson => Selector.Condition, r: Bson => Selector.Condition): Output = args match {
            case _           :: IsBson(v2)  :: Nil =>
              \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(v2)))) }, List(there(0, here))))
            case IsBson(v1)  :: _           :: Nil =>
              \/-(({ case List(f2) => Selector.Doc(ListMap(f2 -> Selector.Expr(r(v1)))) }, List(there(1, here))))
            case _ => -\/(PlannerError.UnsupportedPlan(node))
          }
            
          def stringOp(f: String => Selector.Condition): Output = args match {
            case _           :: IsText(str2) :: Nil => \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(str2)))) }, List(there(0, here))))
            case _ => -\/(PlannerError.UnsupportedPlan(node))
          }

          def invoke2Nel(f: (Selector, Selector) => Selector): Output = {
            val x :: y :: Nil = args.map(_._3)

            (x |@| y) { case ((f1, p1), (f2, p2)) =>
              ({ case list =>
                f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
              },
                p1.map(there(0, _)) ++ p2.map(there(1, _)))
            }
          }

          func match {
            case `Eq`       => relop(Selector.Eq.apply _,  Selector.Eq.apply _)
            case `Neq`      => relop(Selector.Neq.apply _, Selector.Neq.apply _)
            case `Lt`       => relop(Selector.Lt.apply _,  Selector.Gt.apply _)
            case `Lte`      => relop(Selector.Lte.apply _, Selector.Gte.apply _)
            case `Gt`       => relop(Selector.Gt.apply _,  Selector.Lt.apply _)
            case `Gte`      => relop(Selector.Gte.apply _, Selector.Lte.apply _)

            case `IsNull`   => args match {
              case _ :: Nil => \/-((
                { case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Null)) },
                  List(there(0, here))))
              case _ => -\/(PlannerError.UnsupportedPlan(node))
            }

            case `Search`   => stringOp(s => Selector.Regex(s, false, false, false, false))

            case `Between`  => args match {
              case _ :: IsBson(lower) :: IsBson(upper) :: Nil =>
                \/-(({ case List(f) => Selector.And(
                  Selector.Doc(f -> Selector.Gte(lower)),
                  Selector.Doc(f -> Selector.Lte(upper)))
                },
                  List(there(0, here))))
                case _ => -\/(PlannerError.UnsupportedPlan(node))
            }

            case `And`      => invoke2Nel(Selector.And.apply _)
            case `Or`       => invoke2Nel(Selector.Or.apply _)
            // case `Not`      => invoke1(Selector.Not.apply _)

            case _ => -\/(PlannerError.UnsupportedFunction(func))
          }
        }

        val default: PartialSelector[B] = (
          { case List(field) =>
            Selector.Doc(ListMap(
              field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
          },
          List(here))

        node.fold[Output](
          read     = _ => -\/(PlannerError.UnsupportedPlan(node)),
          constant = _ => \/-(default),
          join     = (_, _, _, _, _, _) =>
            -\/(PlannerError.UnsupportedPlan(node)),
          invoke   = invoke(_, _) <+> \/-(default),
          free     = _ => -\/(PlannerError.UnsupportedPlan(node)),
          let      = (_, _, in) => in._3)
      }
    }
  }

  def WorkflowPhase: PhaseS[
    LogicalPlan,
    NameGen,
    (OutputM[PartialSelector[OutputM[WorkflowBuilder]]],
      OutputM[Js.Expr => Js.Expr]),
    OutputM[WorkflowBuilder]] = optimalBoundPhaseS {
      
    import WorkflowBuilder._

    type PSelector = PartialSelector[OutputM[WorkflowBuilder]]
    type Input  = (OutputM[PSelector], OutputM[Js.Expr => Js.Expr])
    type Output = M[WorkflowBuilder]
    type Ann    = Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]

    import LogicalPlan._
    import LogicalPlan.JoinType._
    import Js._
    import Workflow._
    import PlannerError._

    object HasData {
      def unapply(node: Ann): Option[Data] = node match {
        case LogicalPlan.Constant.Attr(data) => Some(data)
        case _ => None
      }
    }

    val HasKeys: Ann => M[List[WorkflowBuilder]] =
      _ match {
        case MakeArrayN.Attr(array) =>
          lift(array.map(_.unFix.attr._2).sequenceU)
        case _ => fail(InternalError("malformed sort keys"))
      }

    val HasSortDirs: Ann => M[List[SortType]] = {
      def isSortDir(node: Ann): M[SortType] =
        node match {
          case HasData(Data.Str("ASC"))  => emit(Ascending)
          case HasData(Data.Str("DESC")) => emit(Descending)
          case _ => fail(InternalError("malformed sort dir"))
        }

      _ match {
        case MakeArrayN.Attr(array) =>
          array.map(isSortDir).sequenceU
          case _ => fail(InternalError("malformed sort dirs"))
      }
    }

    val HasSelector: Ann => M[PSelector] =
      ann => lift(ann.unFix.attr._1._1)

    val HasJs: Ann => M[Js.Expr => Js.Expr] = ann => lift(ann.unFix.attr._1._2)

    val HasWorkflow: Ann => M[WorkflowBuilder] = ann => lift(ann.unFix.attr._2)
    
    val HasAny: Ann => M[Unit] = _ => emit(())
    
    def invoke(func: Func, args: List[Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]]): Output = {

      val HasLiteral: Ann => M[Bson] = ann => HasWorkflow(ann).flatMap { p =>
        asLiteral(p) match {
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
        Arity1(HasWorkflow).flatMap(wb => WorkflowBuilder.expr1(wb)(f))

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output =
        Arity1(HasWorkflow).map(reduce(_)(f))

      def mapExpr(p: WorkflowBuilder)(f: ExprOp => ExprOp): Output =
        WorkflowBuilder.expr1(p)(f)

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output =
        Arity2(HasWorkflow, HasWorkflow).flatMap {
          case (p1, p2) => WorkflowBuilder.expr2(p1, p2)(f)
        }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output =
        Arity3(HasWorkflow, HasWorkflow, HasWorkflow).flatMap {
          case (p1, p2, p3) => WorkflowBuilder.expr3(p1, p2, p3)(f)
        }

      func match {
        case `MakeArray` =>
          Arity1(HasWorkflow).flatMap(x => makeArray(x))
        case `MakeObject` =>
          Arity2(HasText, HasWorkflow).map {
            case (name, wf) => makeObject(wf, name)
          }
        case `ObjectConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => objectConcat(p1, p2)
          }
        case `ArrayConcat` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap {
            case (p1, p2) => arrayConcat(p1, p2)
          }
        case `Filter` =>
          args match {
            case a1 :: a2 :: Nil =>
              HasWorkflow(a1).flatMap(wf =>
                StateT[EitherE, NameGen, WorkflowBuilder](s =>
                  HasSelector(a2).flatMap(s =>
                    lift(s._2.map(_(attrMap(a2)(_._2))).sequenceU).flatMap(filter(wf, _, s._1))).run(s) <+>
                    HasJs(a2).flatMap(js =>
                      filter(wf, Nil, { case Nil => Selector.Where(js(Js.This)) })).run(s)))
            case _ => fail(FuncArity(func, 2))
          }
        case `Drop` =>
          Arity2(HasWorkflow, HasInt64).flatMap((skip(_, _)).tupled)
        case `Take` =>
          Arity2(HasWorkflow, HasInt64).flatMap ((limit(_, _)).tupled)
        case `Cross` =>
          Arity2(HasWorkflow, HasWorkflow).flatMap((cross (_, _)).tupled)
        case `GroupBy` =>
          Arity2(HasWorkflow, HasKeys).flatMap((groupBy(_, _)).tupled)
        case `OrderBy` =>
          Arity3(HasWorkflow, HasKeys, HasSortDirs).flatMap {
            case (p1, p2, dirs) => sortBy(p1, p2, dirs)
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
        
        case `IsNull`     => Arity1(HasWorkflow).flatMap(WorkflowBuilder.expr1(_)(ExprOp.Eq(_, ExprOp.Literal(Bson.Null))))

        case `Coalesce`   => expr2(ExprOp.IfNull.apply _)

        case `Concat`     => expr2(ExprOp.Concat(_, _, Nil))
        case `Lower`      => expr1(ExprOp.ToLower.apply _)
        case `Upper`      => expr1(ExprOp.ToUpper.apply _)
        case `Substring`  => expr3(ExprOp.Substr(_, _, _))

        case `Cond`       => expr3(ExprOp.Cond.apply _)

        case `Count`      => groupExpr1(_ => ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))
        case `Sum`        => groupExpr1(ExprOp.Sum.apply _)
        case `Avg`        => groupExpr1(ExprOp.Avg.apply _)
        case `Min`        => groupExpr1(ExprOp.Min.apply _)
        case `Max`        => groupExpr1(ExprOp.Max.apply _)

        case `Or`         => expr2((a, b) => ExprOp.Or(NonEmptyList.nel(a, b :: Nil)))
        case `And`        => expr2((a, b) => ExprOp.And(NonEmptyList.nel(a, b :: Nil)))
        case `Not`        => expr1(ExprOp.Not.apply)

        case `ArrayLength` =>
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, 1)   => WorkflowBuilder.expr1(p)(ExprOp.Size(_))
            case (_, dim) => fail(FuncApply(func, "lower array dimension", dim.toString))
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

        case `ToTimestamp`   => for {
          str  <- Arity1(HasText)
          date <- lift(parseTimestamp(str))
        } yield WorkflowBuilder.pure(date)

        case `ToInterval`   => for {
          str    <- Arity1(HasText)
          millis <- lift(parseInterval(str))
        } yield WorkflowBuilder.pure(millis)

        case `Between`       => expr3((x, l, u) => ExprOp.And(NonEmptyList.nel(ExprOp.Gte(x, l), ExprOp.Lte(x, u) :: Nil)))

        case `ObjectProject` =>
          Arity2(HasWorkflow, HasText).flatMap {
            case (p, name) => lift(projectField(p, name))
          }
        case `ArrayProject` =>
          Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, index) => projectIndex(p, index.toInt)
          }
        case `FlattenObject` =>
          Arity1(HasWorkflow).flatMap(x => flattenObject(x))
        case `FlattenArray` =>
          Arity1(HasWorkflow).flatMap(x => flattenArray(x))
        case `Squash`       => Arity1(HasWorkflow).map(squash)
        case `Distinct`     =>
          Arity1(HasWorkflow).flatMap(p => distinctBy(p, List(p)))
        case `DistinctBy`   =>
          Arity2(HasWorkflow, HasKeys).flatMap((distinctBy(_, _)).tupled)

        case `Length`       => Arity1(HasWorkflow).flatMap(x => jsExpr1(x, JsMacro(JsCore.Select(_, "length").fix)))

        case `Search`       => Arity2(HasWorkflow, HasWorkflow).flatMap {
          case (value, pattern) =>
            jsExpr2(value, pattern, (v, p) => 
              JsCore.Call(
                JsCore.Select(
                  JsCore.New("RegExp", List(p)).fix,
                  "test").fix,
                List(v)).fix)
        }

        case _ => fail(UnsupportedFunction(func))
      }
    }

    // Tricky: It's easier to implement each step using StateT[\/, ...], but we
    // need the phase’s Monad to be State[..., \/], so that the morphism
    // flatMaps over the State but not the \/. That way it can evaluate to left
    // for an individual node without failing the phase. This code takes care of
    // mapping from one to the other.
    (node: LogicalPlan[Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]]) =>
      node.fold[State[NameGen, Error \/ WorkflowBuilder]](
        read      = path =>
          state(Collection.fromPath(path).map(WorkflowBuilder.read)),

        constant  = data =>
          state(Bson.fromData(data).bimap(
            _ => PlannerError.NonRepresentableData(data),
            WorkflowBuilder.pure)),

        join      = (left, right, tpe, comp, leftKey, rightKey) => {
          def js(attr: Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]): OutputM[Js.Expr => Js.Expr] = attr.unFix.attr._1._2
          def workflow(attr: Attr[LogicalPlan, (Input, Error \/ WorkflowBuilder)]): Error \/ WorkflowBuilder = attr.unFix.attr._2
        
          val rez2 = for {
            l   <- lift(workflow(left))
            r   <- lift(workflow(right))
            lk  <- lift(workflow(leftKey).flatMap(x => asExprOp(x) \/> InternalError("Can’t represent " + x + "as Expr.")))
            rk  <- lift(js(rightKey))
            rez <- join(l, r, tpe, comp, lk, rk)
          } yield rez
          State(s => rez2.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
        },

        invoke    = (func, args) => {
          val v = invoke(func, args)
          State(s => v.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
        },

        free      = _         => sys.error("never reached: boundPhase handles these nodes"),
        let       = (_, _, _) => sys.error("never reached: boundPhase handles these nodes"))
  }
  
  implicit val JsGenRenderTree = new RenderTree[Js.Expr => Js.Expr] { def render(v: Js.Expr => Js.Expr) = Terminal(v(Js.Ident("this")).render(0), List("Js")) }
  def DebugPhase[A: RenderTree] = Phase[LogicalPlan, A, A] { attr => RenderTree.showSwing(attr); attr }

  // SelectorPhase   JsExprPhase
  //              \ /
  //               |
  //         WorkflowPhase
  val AllPhases: PhaseS[LogicalPlan, NameGen, Unit, Error \/ WorkflowBuilder] =
    liftPhaseS(SelectorPhase[Unit, OutputM[WorkflowBuilder]] &&& JsExprPhase[Unit]) >>> WorkflowPhase

  def plan(logical: Term[LogicalPlan]): OutputM[Workflow] = {
    val a: State[NameGen, Attr[LogicalPlan, Error \/ WorkflowBuilder]] = AllPhases(attrUnit(logical))
    swapM(a.map(_.unFix.attr.map(build(_)))).join.evalZero
  }
}
