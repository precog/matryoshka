/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree
import quasar.fp._
import quasar.jscore, jscore.{JsCore, JsFn}

import quasar._
import quasar.fs.Path
import quasar.std.StdLib._
import quasar.javascript._
import Workflow._

import org.threeten.bp.{Duration, Instant}
import scalaz._, Scalaz._

trait Conversions {
  import jscore._

  def jsDate(value: Bson.Date)         = New(Name("Date"), List(Literal(Js.Str(value.value.toString))))
  def jsObjectId(value: Bson.ObjectId) = New(Name("ObjectId"), List(Literal(Js.Str(value.str))))
}
object Conversions extends Conversions

object MongoDbPlanner extends Planner[Crystallized] with Conversions {
  import LogicalPlan._
  import Planner._
  import WorkflowBuilder._

  import quasar.recursionschemes._, Recursive.ops._

  import agg._
  import array._
  import date._
  import identity._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  type InputFinder[A] = Cofree[LogicalPlan, A] => A
  def here[A]: InputFinder[A] = _.head
  def there[A](index: Int, next: InputFinder[A]): InputFinder[A] =
    a => next((Recursive[Cofree[?[_], A]].children(a).apply)(index))
  type Partial[In, Out, A] =
    (PartialFunction[List[In], Out], List[InputFinder[A]])

  type OutputM[A] = PlannerError \/ A

  type PartialJs[A] = Partial[JsFn, JsFn, A]

  def jsExprƒ[B]: LogicalPlan[OutputM[PartialJs[B]]] => OutputM[PartialJs[B]] = {
    type Output = OutputM[PartialJs[B]]

    import jscore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}

    def invoke(func: Func, args: List[Output]): Output = {

      val HasJs: Output => OutputM[PartialJs[B]] =
        _ <+> \/-(({ case List(field) => field }, List(here)))
      val HasStr: Output => OutputM[String] = _.flatMap {
        _._1(Nil)(ident("_")) match {
          case Literal(Js.Str(str)) => \/-(str)
          case x => -\/(FuncApply(func, "JS string", x.toString))
        }
      }

      def Arity1(f: JsCore => JsCore): Output = args match {
        case a1 :: Nil =>
          HasJs(a1).map {
            case (f1, p1) => ({ case list => JsFn(JsFn.defaultName, f(f1(list)(Ident(JsFn.defaultName)))) }, p1.map(there(0, _)))
          }
        case _         => -\/(FuncArity(func, args.length))
      }

      def Arity2(f: (JsCore, JsCore) => JsCore): Output =
        args match {
          case a1 :: a2 :: Nil => (HasJs(a1) |@| HasJs(a2)) {
            case ((f1, p1), (f2, p2)) =>
              ({ case list => JsFn(JsFn.defaultName, f(f1(list.take(p1.size))(Ident(JsFn.defaultName)), f2(list.drop(p1.size))(Ident(JsFn.defaultName)))) },
                p1.map(there(0, _)) ++ p2.map(there(1, _)))
          }
          case _               => -\/(FuncArity(func, args.length))
        }

      def Arity3(
        f: (JsCore, JsCore, JsCore) => JsCore):
          Output = args match {
        case a1 :: a2 :: a3 :: Nil => (HasJs(a1) |@| HasJs(a2) |@| HasJs(a3)) {
          case ((f1, p1), (f2, p2), (f3, p3)) =>
            ({ case list => JsFn(JsFn.defaultName, f(
              f1(list.take(p1.size))(Ident(JsFn.defaultName)),
              f2(list.drop(p1.size).take(p2.size))(Ident(JsFn.defaultName)),
              f3(list.drop(p1.size + p2.size))(Ident(JsFn.defaultName))))
            },
              p1.map(there(0, _)) ++ p2.map(there(1, _)) ++ p3.map(there(2, _)))
        }
        case _                     => -\/(FuncArity(func, args.length))
      }

      def makeSimpleCall(func: String, args: List[JsCore]): JsCore =
        Call(ident(func), args)

      def makeSimpleBinop(op: BinaryOperator): Output =
        Arity2(BinOp(op, _, _))

      def makeSimpleUnop(op: UnaryOperator): Output =
        Arity1(UnOp(op, _))

      func match {
        case Constantly => Arity1(ι)
        case Count => Arity1(Select(_, "count"))
        case Length => Arity1(Select(_, "length"))
        case Sum =>
          Arity1(x =>
            Call(Select(x, "reduce"), List(ident("+"))))
        case Min  =>
          Arity1(x =>
            Call(
              Select(Select(ident("Math"), "min"), "apply"),
              List(Literal(Js.Null), x)))
        case Max  =>
          Arity1(x =>
            Call(
              Select(Select(ident("Math"), "max"), "apply"),
              List(Literal(Js.Null), x)))
        case Add      => makeSimpleBinop(jscore.Add)
        case Multiply => makeSimpleBinop(Mult)
        case Subtract => makeSimpleBinop(Sub)
        case Divide   => makeSimpleBinop(Div)
        case Modulo   => makeSimpleBinop(Mod)
        case Negate   => makeSimpleUnop(Neg)

        case Eq  => makeSimpleBinop(jscore.Eq)
        case Neq => makeSimpleBinop(jscore.Neq)
        case Lt  => makeSimpleBinop(jscore.Lt)
        case Lte => makeSimpleBinop(jscore.Lte)
        case Gt  => makeSimpleBinop(jscore.Gt)
        case Gte => makeSimpleBinop(jscore.Gte)
        case And => makeSimpleBinop(jscore.And)
        case Or  => makeSimpleBinop(jscore.Or)
        case Not => makeSimpleUnop(jscore.Not)
        case IsNull =>
          Arity1(BinOp(jscore.Eq, _, Literal(Js.Null)))
        case In  =>
          Arity2((value, array) =>
            BinOp(jscore.Neq,
              Literal(Js.Num(-1, false)),
              Call(Select(array, "indexOf"), List(value))))
        case Substring =>
          Arity3((field, start, len) =>
            Call(Select(field, "substr"), List(start, len)))
        case Search =>
          Arity2((field, pattern) =>
            Call(Select(New(Name("RegExp"), List(pattern)), "test"),
              List(field)))
        case Extract =>
          args match {
          case a1 :: a2 :: Nil => (HasStr(a1) |@| HasJs(a2)) {
            case (field, source) => ((field match {
              case "century"      => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(100, false))))
              case "day"          => \/-(x => Call(Select(x, "getDate"), Nil)) // (day of month)
              case "decade"       => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(10, false))))
              // Note: MongoDB's Date's getDay (during filtering at least) seems to be monday=0 ... sunday=6,
              // apparently in violation of the JavaScript convention.
              case "dow"          =>
                \/-(x => If(BinOp(jscore.Eq,
                  Call(Select(x, "getDay"), Nil),
                  Literal(Js.Num(6, false))),
                  Literal(Js.Num(0, false)),
                  BinOp(jscore.Add,
                    Call(Select(x, "getDay"), Nil),
                    Literal(Js.Num(1, false)))))
              // TODO: case "doy"          => \/- (???)
              // TODO: epoch
              case "hour"         => \/-(x => Call(Select(x, "getHours"), Nil))
              case "isodow"       =>
                \/-(x => BinOp(jscore.Add,
                  Call(Select(x, "getDay"), Nil),
                  Literal(Js.Num(1, false))))
              // TODO: isoyear
              case "microseconds" =>
                \/-(x => BinOp(Mult,
                  BinOp(jscore.Add,
                    Call(Select(x, "getMilliseconds"), Nil),
                    BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))),
                  Literal(Js.Num(1000, false))))
              case "millennium"   => \/-(x => BinOp(Div, Call(Select(x, "getFullYear"), Nil), Literal(Js.Num(1000, false))))
              case "milliseconds" =>
                \/-(x => BinOp(jscore.Add,
                  Call(Select(x, "getMilliseconds"), Nil),
                  BinOp(Mult, Call(Select(x, "getSeconds"), Nil), Literal(Js.Num(1000, false)))))
              case "minute"       => \/-(x => Call(Select(x, "getMinutes"), Nil))
              case "month"        =>
                \/-(x => BinOp(jscore.Add,
                  Call(Select(x, "getMonth"), Nil),
                  Literal(Js.Num(1, false))))
              case "quarter"      =>
                \/-(x => BinOp(jscore.Add,
                  BinOp(BitOr,
                    BinOp(Div,
                      Call(Select(x, "getMonth"), Nil),
                      Literal(Js.Num(3, false))),
                    Literal(Js.Num(0, false))),
                  Literal(Js.Num(1, false))))
              case "second"       => \/-(x => Call(Select(x, "getSeconds"), Nil))
              // TODO: timezone, timezone_hour, timezone_minute
              // case "week"         => \/- (???)
              case "year"         => \/-(x => Call(Select(x, "getFullYear"), Nil))

              case _ => -\/(FuncApply(func, "valid time period", field))
            }): PlannerError \/ (JsCore => JsCore)).map(x => source.bimap[PartialFunction[List[JsFn], JsFn], List[InputFinder[B]]](
              f1 => { case (list: List[JsFn]) => JsFn(JsFn.defaultName, x(f1(list)(Ident(JsFn.defaultName)))) },
              _.map(there(1, _))))
          }.join
          case _               => -\/(FuncArity(func, args.length))
        }
        case ToId => Arity1(id => Call(ident("ObjectId"), List(id)))
        case Between =>
          Arity3((value, min, max) =>
            makeSimpleCall(
              "&&",
              List(
                makeSimpleCall("<=", List(min, value)),
                makeSimpleCall("<=", List(value, max))))
          )
        case ObjectProject => Arity2(Access(_, _))
        case ArrayProject  => Arity2(Access(_, _))
        case _ => -\/(UnsupportedFunction(func))
      }
    }

    _ match {
      case ConstantF(x)     => \/-(({ case Nil => JsFn.const(x.toJs) }, Nil))
      case InvokeF(f, a)    => invoke(f, a)
      case FreeF(_)         => \/-(({ case List(x) => x }, List(here)))
      case LogicalPlan.LetF(_, _, body) => body
      case x                => -\/(UnsupportedPlan(x, None))
    }
  }

  type PartialSelector[A] = Partial[BsonField, Selector, A]

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
  def selectorƒ[B]:
      LogicalPlan[(Fix[LogicalPlan], OutputM[PartialSelector[B]])] => OutputM[PartialSelector[B]] = { node =>
    type Output = OutputM[PartialSelector[B]]

    object IsBson {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[Bson] =
        v._1.unFix match {
          case ConstantF(b) => BsonCodec.fromData(b).toOption
          case InvokeF(Negate, Fix(ConstantF(Data.Int(i))) :: Nil) => Some(Bson.Int64(-i.toLong))
          case InvokeF(Negate, Fix(ConstantF(Data.Dec(x))) :: Nil) => Some(Bson.Dec(-x.toDouble))
          case InvokeF(ToId, Fix(ConstantF(Data.Str(str))) :: Nil) => Bson.ObjectId(str).toOption
          case _ => None
        }
    }

    object IsText {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    object IsDate {
      def unapply(v: (Fix[LogicalPlan], Output)): Option[Data.Date] =
        v._1.unFix match {
          case ConstantF(d @ Data.Date(_)) => Some(d)
          case _                           => None
        }
    }

    def relMapping(f: Func): Option[Bson => Selector.Condition] = f match {
      case Eq  => Some(Selector.Eq)
      case Neq => Some(Selector.Neq)
      case Lt  => Some(Selector.Lt)
      case Lte => Some(Selector.Lte)
      case Gt  => Some(Selector.Gt)
      case Gte => Some(Selector.Gte)
      case _   => None
    }

    def invoke(func: Func, args: List[(Fix[LogicalPlan], Output)]): Output = {
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

        case _ => -\/(UnsupportedPlan(node, None))
      }

      def relDateOp1(f: Bson.Date => Selector.Condition, date: Data.Date, g: Data.Date => Data.Timestamp, index: Int): Output =
        \/-((
          { case x :: Nil => Selector.Doc(x -> f(Bson.Date(g(date).value))) },
          List(there(index, here))))

      def relDateOp2(conj: (Selector, Selector) => Selector, f1: Bson.Date => Selector.Condition, f2: Bson.Date => Selector.Condition, date: Data.Date, g1: Data.Date => Data.Timestamp, g2: Data.Date => Data.Timestamp, index: Int): Output =
        \/-((
          { case x :: Nil =>
            conj(
              Selector.Doc(x -> f1(Bson.Date(g1(date).value))),
              Selector.Doc(x -> f2(Bson.Date(g2(date).value))))
          },
          List(there(index, here))))

      def stringOp(f: String => Selector.Condition): Output = args match {
        case _           :: IsText(str2) :: Nil => \/-(({ case List(f1) => Selector.Doc(ListMap(f1 -> Selector.Expr(f(str2)))) }, List(there(0, here))))
        case _ => -\/(UnsupportedPlan(node, None))
      }

      def invoke2Nel(f: (Selector, Selector) => Selector): Output = {
        val x :: y :: Nil = args.map(_._2)

        (x |@| y) { case ((f1, p1), (f2, p2)) =>
          ({ case list =>
            f(f1(list.take(p1.size)), f2(list.drop(p1.size)))
          },
            p1.map(there(0, _)) ++ p2.map(there(1, _)))
        }
      }

      def reversibleRelop(f: Mapping): Output =
        (relMapping(f) |@| flip(f).flatMap(relMapping))(relop).getOrElse(-\/(InternalError("couldn’t decipher operation")))

      (func, args) match {
        case (Gt, _ :: IsDate(d2) :: Nil)  => relDateOp1(Selector.Gte, d2, date.startOfNextDay, 0)
        case (Lt, IsDate(d1) :: _ :: Nil)  => relDateOp1(Selector.Gte, d1, date.startOfNextDay, 1)

        case (Lt, _ :: IsDate(d2) :: Nil)  => relDateOp1(Selector.Lt,  d2, date.startOfDay, 0)
        case (Gt, IsDate(d1) :: _ :: Nil)  => relDateOp1(Selector.Lt,  d1, date.startOfDay, 1)

        case (Gte, _ :: IsDate(d2) :: Nil) => relDateOp1(Selector.Gte, d2, date.startOfDay, 0)
        case (Lte, IsDate(d1) :: _ :: Nil) => relDateOp1(Selector.Gte, d1, date.startOfDay, 1)

        case (Lte, _ :: IsDate(d2) :: Nil) => relDateOp1(Selector.Lt,  d2, date.startOfNextDay, 0)
        case (Gte, IsDate(d1) :: _ :: Nil) => relDateOp1(Selector.Lt,  d1, date.startOfNextDay, 1)

        case (Eq, _ :: IsDate(d2) :: Nil) => relDateOp2(Selector.And, Selector.Gte, Selector.Lt, d2, date.startOfDay, date.startOfNextDay, 0)
        case (Eq, IsDate(d1) :: _ :: Nil) => relDateOp2(Selector.And, Selector.Gte, Selector.Lt, d1, date.startOfDay, date.startOfNextDay, 1)

        case (Neq, _ :: IsDate(d2) :: Nil) => relDateOp2(Selector.Or, Selector.Lt, Selector.Gte, d2, date.startOfDay, date.startOfNextDay, 0)
        case (Neq, IsDate(d1) :: _ :: Nil) => relDateOp2(Selector.Or, Selector.Lt, Selector.Gte, d1, date.startOfDay, date.startOfNextDay, 1)

        case (Eq, _)  => reversibleRelop(Eq)
        case (Neq, _) => reversibleRelop(Neq)
        case (Lt, _)  => reversibleRelop(Lt)
        case (Lte, _) => reversibleRelop(Lte)
        case (Gt, _)  => reversibleRelop(Gt)
        case (Gte, _) => reversibleRelop(Gte)

        case (IsNull, _ :: Nil) => \/-((
          { case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Null)) },
          List(there(0, here))))
        case (IsNull, _) => -\/(UnsupportedPlan(node, None))

        case (In, _)  =>
          relop(
            Selector.In.apply _,
            x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

        case (Search, _)   => stringOp(s => Selector.Regex(s, false, false, false, false))

        case (Between, _ :: IsBson(lower) :: IsBson(upper) :: Nil) =>
          \/-(({ case List(f) => Selector.And(
            Selector.Doc(f -> Selector.Gte(lower)),
            Selector.Doc(f -> Selector.Lte(upper)))
          },
            List(there(0, here))))
        case (Between, _) => -\/(UnsupportedPlan(node, None))

        case (And, _)      => invoke2Nel(Selector.And.apply _)
        case (Or, _)       => invoke2Nel(Selector.Or.apply _)
        case (Not, (_, v) :: Nil) =>
          v.map { case (sel, loc) =>
            (sel andThen (s => s.negate)) -> loc.map(there(0, _))
          }

        case (Constantly, const :: _ :: Nil) => const._2

        case _ => -\/(UnsupportedFunction(func))
      }
    }

    val default: PartialSelector[B] = (
      { case List(field) =>
        Selector.Doc(ListMap(
          field -> Selector.Expr(Selector.Eq(Bson.Bool(true)))))
      },
      List(here))

    node match {
      case ConstantF(_)   => \/-(default)
      case InvokeF(f, a)  => invoke(f, a) <+> \/-(default)
      case LetF(_, _, in) => in._2
      case _              => -\/(UnsupportedPlan(node, None))
    }
  }

  val workflowƒ:
      LogicalPlan[
        Cofree[LogicalPlan, (
          (OutputM[PartialSelector[OutputM[WorkflowBuilder]]],
           OutputM[PartialJs[OutputM[WorkflowBuilder]]]),
          OutputM[WorkflowBuilder])]] =>
      State[NameGen, OutputM[WorkflowBuilder]] = {
    import WorkflowBuilder._
    import quasar.physical.mongodb.accumulator._
    import quasar.physical.mongodb.expression._

    type PSelector = PartialSelector[OutputM[WorkflowBuilder]]
    type PJs = PartialJs[OutputM[WorkflowBuilder]]
    type Input  = (OutputM[PSelector], OutputM[PJs])
    type Output = M[WorkflowBuilder]
    type Ann    = Cofree[LogicalPlan, (Input, OutputM[WorkflowBuilder])]

    import LogicalPlan._

    object HasData {
      def unapply(node: LogicalPlan[Ann]): Option[Data] = node match {
        case LogicalPlan.ConstantF(data) => Some(data)
        case _                           => None
      }
    }

    val HasKeys: Ann => OutputM[List[WorkflowBuilder]] = _ match {
      case MakeArrayN.Attr(array) => array.map(_.head._2).sequence
      case n                      => n.head._2.map(List(_))
    }

    val HasSortDirs: Ann => OutputM[List[SortType]] = {
      def isSortDir(node: LogicalPlan[Ann]): OutputM[SortType] =
        node match {
          case HasData(Data.Str("ASC"))  => \/-(Ascending)
          case HasData(Data.Str("DESC")) => \/-(Descending)
          case x => -\/(InternalError("malformed sort dir: " + x))
        }

      _ match {
        case MakeArrayN.Attr(array) =>
          array.map(d => isSortDir(d.tail)).sequence
        case Cofree(_, ConstantF(Data.Arr(dirs))) =>
          dirs.map(d => isSortDir(ConstantF(d))).sequence
        case n => isSortDir(n.tail).map(List(_))
      }
    }

    val HasSelector: Ann => OutputM[PSelector] = _.head._1._1

    val HasJs: Ann => OutputM[PJs] = _.head._1._2

    val HasWorkflow: Ann => OutputM[WorkflowBuilder] = _.head._2

    def invoke(func: Func, args: List[Ann]): Output = {

      val HasLiteral: Ann => OutputM[Bson] = ann => HasWorkflow(ann).flatMap { p =>
        asLiteral(p) match {
          case Some(value) => \/-(value)
          case _           => -\/(FuncApply(func, "literal", p.toString))
        }
      }

      val HasInt64: Ann => OutputM[Long] = HasLiteral(_).flatMap {
        case Bson.Int64(v) => \/-(v)
        case x => -\/(FuncApply(func, "64-bit integer", x.toString))
      }

      val HasText: Ann => OutputM[String] = HasLiteral(_).flatMap {
        case Bson.Text(v) => \/-(v)
        case x => -\/(FuncApply(func, "text", x.toString))
      }

      def Arity1[A](f: Ann => OutputM[A]): OutputM[A] = args match {
        case a1 :: Nil => f(a1)
        case _ => -\/(FuncArity(func, args.length))
      }

      def Arity2[A, B](f1: Ann => OutputM[A], f2: Ann => OutputM[B]): OutputM[(A, B)] = args match {
        case a1 :: a2 :: Nil => (f1(a1) |@| f2(a2))((_, _))
        case _ => -\/(FuncArity(func, args.length))
      }

      def Arity3[A, B, C](f1: Ann => OutputM[A], f2: Ann => OutputM[B], f3: Ann => OutputM[C]):
          OutputM[(A, B, C)] = args match {
        case a1 :: a2 :: a3 :: Nil => (f1(a1) |@| f2(a2) |@| f3(a3))((_, _, _))
        case _ => -\/(FuncArity(func, args.length))
      }

      def expr1(f: Expression => Expression): Output =
        lift(Arity1(HasWorkflow)).flatMap(WorkflowBuilder.expr1(_)(f))

      def groupExpr1(f: Expression => Accumulator): Output =
        lift(Arity1(HasWorkflow).map(reduce(_)(f)))

      def mapExpr(p: WorkflowBuilder)(f: Expression => Expression): Output =
        WorkflowBuilder.expr1(p)(f)

      def expr2[A](f: (Expression, Expression) => Expression): Output =
        lift(Arity2(HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2) => WorkflowBuilder.expr2(p1, p2)(f)
        }

      def expr3(f: (Expression, Expression, Expression) => Expression): Output =
        lift(Arity3(HasWorkflow, HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2, p3) => WorkflowBuilder.expr(List(p1, p2, p3)) {
            case List(e1, e2, e3) => f(e1, e2, e3)
          }
        }

      func match {
        case MakeArray => lift(Arity1(HasWorkflow).map(makeArray))
        case MakeObject =>
          lift(Arity2(HasText, HasWorkflow).map {
            case (name, wf) => makeObject(wf, name)
          })
        case ObjectConcat =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((objectConcat(_, _)).tupled)
        case ArrayConcat =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((arrayConcat(_, _)).tupled)
        case Filter =>
          args match {
            case a1 :: a2 :: Nil =>
              lift(HasWorkflow(a1).flatMap(wf => {
                val on = a2.map(_._2)
                HasSelector(a2).flatMap(s =>
                  s._2.map(_(on)).sequence.map(filter(wf, _, s._1))) <+>
                  HasJs(a2).flatMap(js =>
                    // TODO: have this pass the JS args as the list of inputs … but right now, those inputs get converted to BsonFields, not ExprOps.
                    js._2.map(_(on)).sequence.map(args => filter(wf, Nil, { case Nil => Selector.Where(js._1(args.map(κ(JsFn.identity)))(jscore.ident("this")).toJs) })))
              }))
            case _ => fail(FuncArity(func, args.length))
          }
        case Drop =>
          lift(Arity2(HasWorkflow, HasInt64).map((skip(_, _)).tupled))
        case Take =>
          lift(Arity2(HasWorkflow, HasInt64).map((limit(_, _)).tupled))
        case InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin =>
          args match {
            case List(left, right, comp) =>
              splitConditions(comp).fold[M[WorkflowBuilder]](
                fail(UnsupportedJoinCondition(Recursive[Cofree[?[_], (Input, OutputM[WorkflowBuilder])]].forget(comp))))(
                c => {
                  val (leftKeys, rightKeys) = c.unzip
                  lift((HasWorkflow(left) |@|
                    HasWorkflow(right) |@|
                    leftKeys.map(HasWorkflow).sequenceU |@|
                    leftKeys.map(HasJs).sequenceU |@|
                    rightKeys.map(HasWorkflow).sequenceU |@|
                    rightKeys.map(HasJs).sequenceU)((l, r, lk, lj, rk, rj) =>
                    lift((findArgs(lj, comp) |@| findArgs(rj, comp))((largs, rargs) =>
                      join(l, r, func, lk, applyPartials(lj, largs), rk, applyPartials(rj, rargs)))).join)).join
                })
            case _ => fail(FuncArity(func, args.length))
          }
        case GroupBy =>
          lift(Arity2(HasWorkflow, HasKeys).map((groupBy(_, _)).tupled))
        case OrderBy =>
          lift(Arity3(HasWorkflow, HasKeys, HasSortDirs).map {
            case (p1, p2, dirs) => sortBy(p1, p2, dirs)
          })

        case Constantly => expr2((v, s) => v)

        case Add        => expr2($add(_, _))
        case Multiply   => expr2($multiply(_, _))
        case Subtract   => expr2($subtract(_, _))
        case Divide     => expr2($divide(_, _))
        case Modulo     => expr2($mod(_, _))
        case Negate     => expr1($multiply($literal(Bson.Int32(-1)), _))

        case Eq         => expr2($eq(_, _))
        case Neq        => expr2($neq(_, _))
        case Lt         => expr2($lt(_, _))
        case Lte        => expr2($lte(_, _))
        case Gt         => expr2($gt(_, _))
        case Gte        => expr2($gte(_, _))

        case IsNull     =>
          lift(Arity1(HasWorkflow)).flatMap(
            mapExpr(_)($eq(_, $literal(Bson.Null))))

        case Coalesce   => expr2($ifNull(_, _))

        case Concat     => expr2($concat(_, _))
        case Lower      => expr1($toLower(_))
        case Upper      => expr1($toUpper(_))
        case Substring  => expr3($substr(_, _, _))

        case Cond       => expr3($cond(_, _, _))

        case Count      => groupExpr1(κ($sum($literal(Bson.Int32(1)))))
        case Sum        => groupExpr1($sum(_))
        case Avg        => groupExpr1($avg(_))
        case Min        => groupExpr1($min(_))
        case Max        => groupExpr1($max(_))
        case Arbitrary  => groupExpr1($first(_))

        case Or         => expr2($or(_, _))
        case And        => expr2($and(_, _))
        case Not        => expr1($not(_))

        case ArrayLength =>
          lift(Arity2(HasWorkflow, HasInt64)).flatMap {
            case (p, 1)   => mapExpr(p)($size(_))
            case (_, dim) => fail(FuncApply(func, "lower array dimension", dim.toString))
          }

        case Extract   =>
          lift(Arity2(HasText, HasWorkflow)).flatMap {
            case (field, p) =>
              field match {
                case "century"      =>
                  mapExpr(p)(v => $divide($year(v), $literal(Bson.Int32(100))))
                case "day"          => mapExpr(p)($dayOfMonth(_))
                case "decade"       =>
                  mapExpr(p)(x => $divide($year(x), $literal(Bson.Int64(10))))
                case "dow"          =>
                  mapExpr(p)(x => $add($dayOfWeek(x), $literal(Bson.Int64(-1))))
                case "doy"          => mapExpr(p)($dayOfYear(_))
                // TODO: epoch
                case "hour"         => mapExpr(p)($hour(_))
                case "isodow"       => mapExpr(p)(x =>
                  $cond($eq($dayOfWeek(x), $literal(Bson.Int64(1))),
                    $literal(Bson.Int64(7)),
                    $add($dayOfWeek(x), $literal(Bson.Int64(-1)))))
                // TODO: isoyear
                case "microseconds" =>
                  mapExpr(p)(v =>
                    $multiply($millisecond(v), $literal(Bson.Int32(1000))))
                case "millennium"   =>
                  mapExpr(p)(v => $divide($year(v), $literal(Bson.Int32(1000))))
                case "milliseconds" => mapExpr(p)($millisecond(_))
                case "minute"       => mapExpr(p)($minute(_))
                case "month"        => mapExpr(p)($month(_))
                case "quarter"      => // TODO: handle leap years
                  mapExpr(p)(v =>
                    $add(
                      $divide($dayOfYear(v), $literal(Bson.Int32(92))),
                      $literal(Bson.Int32(1))))
                case "second"       => mapExpr(p)($second(_))
                // TODO: timezone, timezone_hour, timezone_minute
                case "week"         => mapExpr(p)($week(_))
                case "year"         => mapExpr(p)($year(_))
                case _              => fail(FuncApply(func, "valid time period", field))
              }
          }

        case TimeOfDay    => {
          def pad2(x: JsCore) =
            jscore.Let(jscore.Name("x"), x,
              jscore.If(
                jscore.BinOp(jscore.Lt, jscore.ident("x"), jscore.Literal(Js.Num(10, false))),
                jscore.BinOp(jscore.Add, jscore.Literal(Js.Str("0")), jscore.ident("x")),
                jscore.ident("x")))
          def pad3(x: JsCore) =
            jscore.Let(jscore.Name("x"), x,
              jscore.If(
                jscore.BinOp(jscore.Lt, jscore.ident("x"), jscore.Literal(Js.Num(100, false))),
                jscore.BinOp(jscore.Add, jscore.Literal(Js.Str("00")), jscore.ident("x")),
                jscore.If(
                  jscore.BinOp(jscore.Lt, jscore.ident("x"), jscore.Literal(Js.Num(10, false))),
                  jscore.BinOp(jscore.Add, jscore.Literal(Js.Str("0")), jscore.ident("x")),
                  jscore.ident("x"))))
          lift(Arity1(HasWorkflow).flatMap(wb => jsExpr1(wb, JsFn(JsFn.defaultName,
            jscore.Let(jscore.Name("t"), jscore.Ident(JsFn.defaultName),
              jscore.binop(jscore.Add,
                pad2(jscore.Call(jscore.Select(jscore.ident("t"), "getUTCHours"), Nil)),
                jscore.Literal(Js.Str(":")),
                pad2(jscore.Call(jscore.Select(jscore.ident("t"), "getUTCMinutes"), Nil)),
                jscore.Literal(Js.Str(":")),
                pad2(jscore.Call(jscore.Select(jscore.ident("t"), "getUTCSeconds"), Nil)),
                jscore.Literal(Js.Str(".")),
                pad3(jscore.Call(jscore.Select(jscore.ident("t"), "getUTCMilliseconds"), Nil))))))))
        }

        case ToTimestamp => expr1($add($literal(Bson.Date(Instant.ofEpochMilli(0))), _))

        case ToId         => lift(args match {
          case a1 :: Nil =>
            HasText(a1).flatMap(str => BsonCodec.fromData(Data.Id(str)).map(WorkflowBuilder.pure)) <+>
              HasWorkflow(a1).flatMap(src => jsExpr1(src, JsFn(JsFn.defaultName, jscore.Call(jscore.ident("ObjectId"), List(jscore.Ident(JsFn.defaultName))))))
          case _ => -\/(FuncArity(func, args.length))
        })

        case Between       => expr3((x, l, u) => $and($lte(l, x), $lte(x, u)))

        case ObjectProject =>
          lift(Arity2(HasWorkflow, HasText).flatMap((projectField(_, _)).tupled))
        case ArrayProject =>
          lift(Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, index) => projectIndex(p, index.toInt)
          })
        case DeleteField  =>
          lift(Arity2(HasWorkflow, HasText).flatMap((deleteField(_, _)).tupled))
        case FlattenObject => lift(Arity1(HasWorkflow)).flatMap(flattenObject)
        case FlattenArray => lift(Arity1(HasWorkflow)).flatMap(flattenArray)
        case Squash       => lift(Arity1(HasWorkflow).map(squash))
        case Distinct     =>
          lift(Arity1(HasWorkflow)).flatMap(p => distinctBy(p, List(p)))
        case DistinctBy   =>
          lift(Arity2(HasWorkflow, HasKeys)).flatMap((distinctBy(_, _)).tupled)

        case Length       =>
          lift(Arity1(HasWorkflow).flatMap(jsExpr1(_, JsFn(JsFn.defaultName, jscore.Select(jscore.Ident(JsFn.defaultName), "length")))))

        case Search       => lift(Arity2(HasWorkflow, HasWorkflow)).flatMap {
          case (value, pattern) =>
            jsExpr2(value, pattern, (v, p) =>
              jscore.Call(
                jscore.Select(
                  jscore.New(jscore.Name("RegExp"), List(p)),
                  "test"),
                List(v)))
        }

        case _ => fail(UnsupportedFunction(func))
      }
    }

    def splitConditions: Ann => Option[List[(Ann, Ann)]] =
      _.tail match {
      case InvokeF(relations.And, terms) =>
        terms.map(splitConditions).sequence.map(_.concatenate)
      case InvokeF(relations.Eq, List(left, right)) => Some(List((left, right)))
      case ConstantF(Data.Bool(true)) => Some(List())
      case _ => None
    }

    def findArgs(partials: List[PJs], comp: Ann):
        OutputM[List[List[WorkflowBuilder]]] =
      partials.map(_._2.map(_(comp.map(_._2))).sequenceU).sequenceU

    def applyPartials(partials: List[PJs], args: List[List[WorkflowBuilder]]):
        List[JsFn] =
      (partials zip args).map(l => l._1._1(l._2.map(κ(JsFn.identity))))

    // Tricky: It's easier to implement each step using StateT[\/, ...], but we
    // need the fold’s Monad to be State[..., \/], so that the morphism
    // flatMaps over the State but not the \/. That way it can evaluate to left
    // for an individual node without failing the fold. This code takes care of
    // mapping from one to the other.
    _ match {
      case ReadF(path) =>
        state(Collection.fromPath(path).bimap(PlanPathError, WorkflowBuilder.read))
      case ConstantF(data) =>
        state(BsonCodec.fromData(data).bimap(
          κ(NonRepresentableData(data)),
          WorkflowBuilder.pure))
      case InvokeF(func, args) =>
        val v = invoke(func, args)
        State(s => v.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
      case FreeF(name) =>
        state(-\/(InternalError("variable " + name + " is unbound")))
      case LetF(_, _, in) => state(in.head._2)
    }
  }

  import Planner._

  val annotateƒ = zipPara(
    selectorƒ[OutputM[WorkflowBuilder]],
    liftPara(jsExprƒ[OutputM[WorkflowBuilder]]))

  def alignJoinsƒ:
      LogicalPlan[Fix[LogicalPlan]] => OutputM[Fix[LogicalPlan]] = {
    def containsTableRefs(condA: Fix[LogicalPlan], tableA: Fix[LogicalPlan], condB: Fix[LogicalPlan], tableB: Fix[LogicalPlan]) =
      condA.contains(tableA) && condB.contains(tableB) &&
        condA.all(_ ≠ tableB) && condB.all(_ ≠ tableA)
    def alignCondition(lt: Fix[LogicalPlan], rt: Fix[LogicalPlan]):
        Fix[LogicalPlan] => OutputM[Fix[LogicalPlan]] =
      _.unFix match {
        case InvokeF(And, terms) =>
          terms.map(alignCondition(lt, rt)).sequenceU.map(Invoke(And, _))
        case InvokeF(Or, terms) =>
          terms.map(alignCondition(lt, rt)).sequenceU.map(Invoke(Or, _))
        case InvokeF(Not, terms) =>
          terms.map(alignCondition(lt, rt)).sequenceU.map(Invoke(Not, _))
        case x @ InvokeF(func: Mapping, List(left, right)) =>
          if (containsTableRefs(left, lt, right, rt))
            \/-(Invoke(func, List(left, right)))
          else if (containsTableRefs(left, rt, right, lt))
            flip(func).fold[PlannerError \/ Fix[LogicalPlan]](
              -\/(UnsupportedJoinCondition(Fix(x))))(
              f => \/-(Invoke(f, List(right, left))))
          else -\/(UnsupportedJoinCondition(Fix(x)))
        case x => \/-(Fix(x))
      }

    {
      case InvokeF(f @ (InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin), List(l, r, cond)) =>
        alignCondition(l, r)(cond).map(c => Invoke(f, List(l, r, c)))
      case x => \/-(Fix(x))
    }
  }

  def plan(logical: Fix[LogicalPlan]): EitherWriter[PlannerError, Crystallized] = {
    // NB: locally add state on top of the result monad so everything
    // can be done in a single for comprehension.
    type M[A] = StateT[EitherT[(Vector[PhaseResult], ?), PlannerError, ?], NameGen, A]
    // NB: cannot resolve the implicits, for mysterious reasons (H-K type inference)
    implicit val F: Monad[EitherT[(Vector[PhaseResult], ?), PlannerError, ?]] =
      EitherT.eitherTMonad[(Vector[PhaseResult], ?), PlannerError]
    implicit val A: Applicative[StateT[EitherT[(Vector[PhaseResult], ?), PlannerError, ?], NameGen, ?]] =
      StateT.stateTMonadState[NameGen, EitherT[(Vector[PhaseResult], ?), PlannerError, ?]]

    def log[A: RenderTree](label: String)(ma: M[A]): M[A] =
      ma.flatMap { a =>
        val result = PhaseResult.Tree(label, RenderTree[A].render(a))
        StateT[EitherT[(Vector[PhaseResult], ?), PlannerError, ?], NameGen, A]((ng: NameGen) =>
          EitherT[(Vector[PhaseResult], ?), PlannerError, (NameGen, A)](
            (Vector(result), \/-(ng -> a))))
      }

    def swizzle[A](sa: StateT[PlannerError \/ ?, NameGen, A]): M[A] =
      StateT[EitherT[(Vector[PhaseResult], ?), PlannerError, ?], NameGen, A] { (ng: NameGen) =>
        EitherT[(Vector[PhaseResult], ?), PlannerError, (NameGen, A)](
          (Vector.empty, sa.run(ng)))
      }

    def stateT[F[_]: Functor, S, A](fa: F[A]) =
      StateT[F, S, A](s => fa.map((s, _)))

    val wfƒ = workflowƒ andThen (s => s.map(_.map(normalize)))

    (for {
      align <- log("Logical Plan (aligned joins)")       (swizzle(stateT(logical.cataM(alignJoinsƒ))))
      prep <- log("Logical Plan (projections preferred)")(Optimizer.preferProjections(align).point[M])
      wb   <- log("Workflow Builder")                    (swizzle(swapM(lpParaZygoHistoS(prep)(annotateƒ, wfƒ))))
      wf1  <- log("Workflow (raw)")                      (swizzle(build(wb)))
      wf2  <- log("Workflow (finished)")                 (finish(wf1).point[M])
    } yield crystallize(wf2)).evalZero
  }
}
