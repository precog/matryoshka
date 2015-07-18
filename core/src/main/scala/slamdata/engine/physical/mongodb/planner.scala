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

package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine.std.StdLib._
import slamdata.engine.javascript._
import Workflow._

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.threeten.bp.{Duration, Instant}

trait Conversions {
  import JsCore._

  def jsDate(value: Bson.Date)         = New("Date", List(Literal(Js.Str(value.value.toString)).fix)).fix
  def jsObjectId(value: Bson.ObjectId) = New("ObjectId", List(Literal(Js.Str(value.str)).fix)).fix
}
object Conversions extends Conversions

object MongoDbPlanner extends Planner[Crystallized] with Conversions {
  import LogicalPlan._
  import WorkflowBuilder._

  import slamdata.engine.analysis.fixplate._

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
    a => next((children(a).apply)(index))
  type Partial[In, Out, A] =
    (PartialFunction[List[In], Out], List[InputFinder[A]])

  type OutputM[A] = Error \/ A

  type PartialJs[A] = Partial[JsFn, JsFn, A]

  def jsExprƒ[B]: LogicalPlan[OutputM[PartialJs[B]]] => OutputM[PartialJs[B]] = {
    type Output = OutputM[PartialJs[B]]

    import JsCore.{
      Add => _, In => _,
      Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
      And => _, Or => _, Not => _,
      _}
    import PlannerError._

    def invoke(func: Func, args: List[Output]): Output = {

      val HasJs: Output => OutputM[PartialJs[B]] =
        _ <+> \/-(({ case List(field) => field }, List(here)))
      val HasStr: Output => OutputM[String] = _.flatMap {
        _._1(Nil)(Ident("_").fix).unFix match {
          case Literal(Js.Str(str)) => \/-(str)
          case x => -\/(FuncApply(func, "JS string", x.toString))
        }
      }

      def Arity1(f: Term[JsCore] => Term[JsCore]): Output = args match {
        case a1 :: Nil =>
          HasJs(a1).map {
            case (f1, p1) => ({ case list => JsFn(JsFn.base, f(f1(list)(JsFn.base.fix))) }, p1.map(there(0, _)))
          }
        case _         => -\/(FuncArity(func, args.length))
      }

      def Arity2(f: (Term[JsCore], Term[JsCore]) => Term[JsCore]): Output =
        args match {
          case a1 :: a2 :: Nil => (HasJs(a1) |@| HasJs(a2)) {
            case ((f1, p1), (f2, p2)) =>
              ({ case list => JsFn(JsFn.base, f(f1(list.take(p1.size))(JsFn.base.fix), f2(list.drop(p1.size))(JsFn.base.fix))) },
                p1.map(there(0, _)) ++ p2.map(there(1, _)))
          }
          case _               => -\/(FuncArity(func, args.length))
        }

      def Arity3(
        f: (Term[JsCore], Term[JsCore], Term[JsCore]) => Term[JsCore]):
          Output = args match {
        case a1 :: a2 :: a3 :: Nil => (HasJs(a1) |@| HasJs(a2) |@| HasJs(a3)) {
          case ((f1, p1), (f2, p2), (f3, p3)) =>
            ({ case list => JsFn(JsFn.base, f(
              f1(list.take(p1.size))(JsFn.base.fix),
              f2(list.drop(p1.size).take(p2.size))(JsFn.base.fix),
              f3(list.drop(p1.size + p2.size))(JsFn.base.fix)))
            },
              p1.map(there(0, _)) ++ p2.map(there(1, _)) ++ p3.map(there(2, _)))
        }
        case _                     => -\/(FuncArity(func, args.length))
      }

      def makeSimpleCall(func: String, args: List[Term[JsCore]]): Term[JsCore] =
        Call(Ident(func).fix, args).fix

      def makeSimpleBinop(op: BinaryOperator): Output =
        Arity2(BinOp(op, _, _).fix)

      def makeSimpleUnop(op: UnaryOperator): Output =
        Arity1(UnOp(op, _).fix)

      func match {
        case `Constantly` => Arity1(ɩ)
        case `Count` => Arity1(Select(_, "count").fix)
        case `Length` => Arity1(Select(_, "length").fix)
        case `Sum` =>
          Arity1(x =>
            Call(Select(x, "reduce").fix, List(Ident("+").fix)).fix)
        case `Min`  =>
          Arity1(x =>
            Call(
              Select(Select(Ident("Math").fix, "min").fix, "apply").fix,
              List(Literal(Js.Null).fix, x)).fix)
        case `Max`  =>
          Arity1(x =>
            Call(
              Select(Select(Ident("Math").fix, "max").fix, "apply").fix,
              List(Literal(Js.Null).fix, x)).fix)
        case `Add`      => makeSimpleBinop(JsCore.Add)
        case `Multiply` => makeSimpleBinop(Mult)
        case `Subtract` => makeSimpleBinop(Sub)
        case `Divide`   => makeSimpleBinop(Div)
        case `Modulo`   => makeSimpleBinop(Mod)
        case `Negate`   => makeSimpleUnop(Neg)

        case `Eq`  => makeSimpleBinop(JsCore.Eq)
        case `Neq` => makeSimpleBinop(JsCore.Neq)
        case `Lt`  => makeSimpleBinop(JsCore.Lt)
        case `Lte` => makeSimpleBinop(JsCore.Lte)
        case `Gt`  => makeSimpleBinop(JsCore.Gt)
        case `Gte` => makeSimpleBinop(JsCore.Gte)
        case `And` => makeSimpleBinop(JsCore.And)
        case `Or`  => makeSimpleBinop(JsCore.Or)
        case `Not` => makeSimpleUnop(JsCore.Not)
        case `IsNull` =>
          Arity1(BinOp(JsCore.Eq, _, Literal(Js.Null).fix).fix)
        case `In`  =>
          Arity2((value, array) =>
            BinOp(JsCore.Neq,
              Literal(Js.Num(-1, false)).fix,
              Call(Select(array, "indexOf").fix, List(value)).fix).fix)
        case `Substring` =>
          Arity3((field, start, len) =>
            Call(Select(field, "substr").fix, List(start, len)).fix)
        case `Search` =>
          Arity2((field, pattern) =>
            Call(Select(New("RegExp", List(pattern)).fix, "test").fix,
              List(field)).fix)
        case `Extract` =>
          args match {
          case a1 :: a2 :: Nil => (HasStr(a1) |@| HasJs(a2)) {
            case (field, source) => ((field match {
              case "century"      => \/-(x => BinOp(Div, Call(Select(x, "getFullYear").fix, Nil).fix, Literal(Js.Num(100, false)).fix).fix)
              case "day"          => \/-(x => Call(Select(x, "getDate").fix, Nil).fix) // (day of month)
              case "decade"       => \/-(x => BinOp(Div, Call(Select(x, "getFullYear").fix, Nil).fix, Literal(Js.Num(10, false)).fix).fix)
              // Note: MongoDB's Date's getDay (during filtering at least) seems to be monday=0 ... sunday=6,
              // apparently in violation of the JavaScript convention.
              case "dow"          =>
                \/-(x => If(BinOp(JsCore.Eq,
                  Call(Select(x, "getDay").fix, Nil).fix,
                  Literal(Js.Num(6, false)).fix).fix,
                  Literal(Js.Num(0, false)).fix,
                  BinOp(JsCore.Add,
                    Call(Select(x, "getDay").fix, Nil).fix,
                    Literal(Js.Num(1, false)).fix).fix).fix)
              // TODO: case "doy"          => \/- (???)
              // TODO: epoch
              case "hour"         => \/-(x => Call(Select(x, "getHours").fix, Nil).fix)
              case "isodow"       =>
                \/-(x => BinOp(JsCore.Add,
                  Call(Select(x, "getDay").fix, Nil).fix,
                  Literal(Js.Num(1, false)).fix).fix)
              // TODO: isoyear
              case "microseconds" =>
                \/-(x => BinOp(Mult,
                  BinOp(JsCore.Add,
                    Call(Select(x, "getMilliseconds").fix, Nil).fix,
                    BinOp(Mult, Call(Select(x, "getSeconds").fix, Nil).fix, Literal(Js.Num(1000, false)).fix).fix).fix,
                  Literal(Js.Num(1000, false)).fix).fix)
              case "millennium"   => \/-(x => BinOp(Div, Call(Select(x, "getFullYear").fix, Nil).fix, Literal(Js.Num(1000, false)).fix).fix)
              case "milliseconds" =>
                \/-(x => BinOp(JsCore.Add,
                  Call(Select(x, "getMilliseconds").fix, Nil).fix,
                  BinOp(Mult, Call(Select(x, "getSeconds").fix, Nil).fix, Literal(Js.Num(1000, false)).fix).fix).fix)
              case "minute"       => \/-(x => Call(Select(x, "getMinutes").fix, Nil).fix)
              case "month"        =>
                \/-(x => BinOp(JsCore.Add,
                  Call(Select(x, "getMonth").fix, Nil).fix,
                  Literal(Js.Num(1, false)).fix).fix)
              case "quarter"      =>
                \/-(x => BinOp(JsCore.Add,
                  BinOp(BitOr,
                    BinOp(Div,
                      Call(Select(x, "getMonth").fix, Nil).fix,
                      Literal(Js.Num(3, false)).fix).fix,
                    Literal(Js.Num(0, false)).fix).fix,
                  Literal(Js.Num(1, false)).fix).fix)
              case "second"       => \/-(x => Call(Select(x, "getSeconds").fix, Nil).fix)
              // TODO: timezone, timezone_hour, timezone_minute
              // case "week"         => \/- (???)
              case "year"         => \/-(x => Call(Select(x, "getFullYear").fix, Nil).fix)

              case _ => -\/(FuncApply(func, "valid time period", field))
            }): Error \/ (Term[JsCore] => Term[JsCore])).map(x => source.bimap[PartialFunction[List[JsFn], JsFn], List[InputFinder[B]]](
              f1 => { case (list: List[JsFn]) => JsFn(JsFn.base, x(f1(list)(JsFn.base.fix))) },
              _.map(there(1, _))))
          }.join
          case _               => -\/(FuncArity(func, args.length))
        }
        case `ToId` => Arity1(id => Call(Ident("ObjectId").fix, List(id)).fix)
        case `Between` =>
          Arity3((value, min, max) =>
            makeSimpleCall(
              "&&",
              List(
                makeSimpleCall("<=", List(min, value)),
                makeSimpleCall("<=", List(value, max))))
          )
        case `ObjectProject` => Arity2(Access(_, _).fix)
        case `ArrayProject`  => Arity2(Access(_, _).fix)
        case _ => -\/(UnsupportedFunction(func))
      }
    }

    _ match {
      case ConstantF(x)     => \/-(({ case Nil => JsFn.const(x.toJs) }, Nil))
      case InvokeF(f, a)    => invoke(f, a)
      case FreeF(_)         => \/-(({ case List(x) => x }, List(here)))
      case LetF(_, _, body) => body
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
      LogicalPlan[(Term[LogicalPlan], OutputM[PartialSelector[B]])] => OutputM[PartialSelector[B]] = { node =>
    type Output = OutputM[PartialSelector[B]]

    object IsBson {
      def unapply(v: (Term[LogicalPlan], Output)): Option[Bson] =
        v._1.unFix match {
          case ConstantF(b) => BsonCodec.fromData(b).toOption
          case InvokeF(Negate, Term(ConstantF(Data.Int(i))) :: Nil) => Some(Bson.Int64(-i.toLong))
          case InvokeF(Negate, Term(ConstantF(Data.Dec(x))) :: Nil) => Some(Bson.Dec(-x.toDouble))
          case InvokeF(ToId, Term(ConstantF(Data.Str(str))) :: Nil) => Bson.ObjectId(str).toOption
          case _ => None
        }
    }

    object IsText {
      def unapply(v: (Term[LogicalPlan], Output)): Option[String] =
        v match {
          case IsBson(Bson.Text(str)) => Some(str)
          case _                      => None
        }
    }

    object IsDate {
      def unapply(v: (Term[LogicalPlan], Output)): Option[Data.Date] =
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

    def invoke(func: Func, args: List[(Term[LogicalPlan], Output)]): Output = {
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

        case _ => -\/(PlannerError.UnsupportedPlan(node, None))
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
        case _ => -\/(PlannerError.UnsupportedPlan(node, None))
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
        (relMapping(f) |@| flip(f).flatMap(relMapping))(relop).getOrElse(-\/(PlannerError.InternalError("couldn’t decipher operation")))

      (func, args) match {
        case (`Gt`, _ :: IsDate(d2) :: Nil)  => relDateOp1(Selector.Gte, d2, date.startOfNextDay, 0)
        case (`Lt`, IsDate(d1) :: _ :: Nil)  => relDateOp1(Selector.Gte, d1, date.startOfNextDay, 1)

        case (`Lt`, _ :: IsDate(d2) :: Nil)  => relDateOp1(Selector.Lt,  d2, date.startOfDay, 0)
        case (`Gt`, IsDate(d1) :: _ :: Nil)  => relDateOp1(Selector.Lt,  d1, date.startOfDay, 1)

        case (`Gte`, _ :: IsDate(d2) :: Nil) => relDateOp1(Selector.Gte, d2, date.startOfDay, 0)
        case (`Lte`, IsDate(d1) :: _ :: Nil) => relDateOp1(Selector.Gte, d1, date.startOfDay, 1)

        case (`Lte`, _ :: IsDate(d2) :: Nil) => relDateOp1(Selector.Lt,  d2, date.startOfNextDay, 0)
        case (`Gte`, IsDate(d1) :: _ :: Nil) => relDateOp1(Selector.Lt,  d1, date.startOfNextDay, 1)

        case (`Eq`, _ :: IsDate(d2) :: Nil) => relDateOp2(Selector.And, Selector.Gte, Selector.Lt, d2, date.startOfDay, date.startOfNextDay, 0)
        case (`Eq`, IsDate(d1) :: _ :: Nil) => relDateOp2(Selector.And, Selector.Gte, Selector.Lt, d1, date.startOfDay, date.startOfNextDay, 1)

        case (`Neq`, _ :: IsDate(d2) :: Nil) => relDateOp2(Selector.Or, Selector.Lt, Selector.Gte, d2, date.startOfDay, date.startOfNextDay, 0)
        case (`Neq`, IsDate(d1) :: _ :: Nil) => relDateOp2(Selector.Or, Selector.Lt, Selector.Gte, d1, date.startOfDay, date.startOfNextDay, 1)

        case (`Eq`, _)  => reversibleRelop(Eq)
        case (`Neq`, _) => reversibleRelop(Neq)
        case (`Lt`, _)  => reversibleRelop(Lt)
        case (`Lte`, _) => reversibleRelop(Lte)
        case (`Gt`, _)  => reversibleRelop(Gt)
        case (`Gte`, _) => reversibleRelop(Gte)

        case (`IsNull`, _ :: Nil) => \/-((
          { case f :: Nil => Selector.Doc(f -> Selector.Eq(Bson.Null)) },
          List(there(0, here))))
        case (`IsNull`, _) => -\/(PlannerError.UnsupportedPlan(node, None))

        case (`In`, _)  =>
          relop(
            Selector.In.apply _,
            x => Selector.ElemMatch(\/-(Selector.In(Bson.Arr(List(x))))))

        case (`Search`, _)   => stringOp(s => Selector.Regex(s, false, false, false, false))

        case (`Between`, _ :: IsBson(lower) :: IsBson(upper) :: Nil) =>
          \/-(({ case List(f) => Selector.And(
            Selector.Doc(f -> Selector.Gte(lower)),
            Selector.Doc(f -> Selector.Lte(upper)))
          },
            List(there(0, here))))
        case (`Between`, _) => -\/(PlannerError.UnsupportedPlan(node, None))

        case (`And`, _)      => invoke2Nel(Selector.And.apply _)
        case (`Or`, _)       => invoke2Nel(Selector.Or.apply _)
          // case (`Not`, _)      => invoke1(Selector.Not.apply _)

        case (`Constantly`, const :: _ :: Nil) => const._2

        case _ => -\/(PlannerError.UnsupportedFunction(func))
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
      case _              => -\/(PlannerError.UnsupportedPlan(node, None))
    }
  }

  val workflowƒ:
      LogicalPlan[(
        (OutputM[PartialSelector[OutputM[WorkflowBuilder]]],
          OutputM[PartialJs[OutputM[WorkflowBuilder]]]),
        Cofree[LogicalPlan, OutputM[WorkflowBuilder]])] =>
      State[NameGen, OutputM[WorkflowBuilder]] = {
    import WorkflowBuilder._

    type PSelector = PartialSelector[OutputM[WorkflowBuilder]]
    type PJs = PartialJs[OutputM[WorkflowBuilder]]
    type Input  = (OutputM[PSelector], OutputM[PJs])
    type Output = M[WorkflowBuilder]
    type Ann    = (Input, Cofree[LogicalPlan, OutputM[WorkflowBuilder]])

    import LogicalPlan._
    import PlannerError._

    object HasData {
      def unapply(node: LogicalPlan[Cofree[LogicalPlan, OutputM[WorkflowBuilder]]]): Option[Data] = node match {
        case LogicalPlan.ConstantF(data) => Some(data)
        case _                           => None
      }
    }

    val HasKeys: Ann => OutputM[List[WorkflowBuilder]] = _._2 match {
      case MakeArrayN.Attr(array) => array.map(_.head).sequence
      case n => n.head.map(List(_))
    }

    val HasSortDirs: Ann => OutputM[List[SortType]] = {
      def isSortDir(node: LogicalPlan[Cofree[LogicalPlan, OutputM[WorkflowBuilder]]]): OutputM[SortType] =
        node match {
          case HasData(Data.Str("ASC"))  => \/-(Ascending)
          case HasData(Data.Str("DESC")) => \/-(Descending)
          case x => -\/(InternalError("malformed sort dir: " + x))
        }

      _._2 match {
        case MakeArrayN.Attr(array) =>
          array.map(d => isSortDir(d.tail)).sequence
        case Cofree(_, ConstantF(Data.Arr(dirs))) =>
          dirs.map(d => isSortDir(ConstantF(d))).sequence
        case n => isSortDir(n.tail).map(List(_))
      }
    }

    val HasSelector: Ann => OutputM[PSelector] = _._1._1

    val HasJs: Ann => OutputM[PJs] = _._1._2

    val HasWorkflow: Ann => OutputM[WorkflowBuilder] = _._2.head

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

      def expr1(f: ExprOp => ExprOp): Output =
        lift(Arity1(HasWorkflow)).flatMap(WorkflowBuilder.expr1(_)(f))

      def groupExpr1(f: ExprOp => ExprOp.GroupOp): Output =
        lift(Arity1(HasWorkflow).map(reduce(_)(f)))

      def mapExpr(p: WorkflowBuilder)(f: ExprOp => ExprOp): Output =
        WorkflowBuilder.expr1(p)(f)

      def expr2(f: (ExprOp, ExprOp) => ExprOp): Output =
        lift(Arity2(HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2) => WorkflowBuilder.expr2(p1, p2)(f)
        }

      def expr3(f: (ExprOp, ExprOp, ExprOp) => ExprOp): Output =
        lift(Arity3(HasWorkflow, HasWorkflow, HasWorkflow)).flatMap {
          case (p1, p2, p3) => WorkflowBuilder.expr(List(p1, p2, p3)) {
            case List(e1, e2, e3) => f(e1, e2, e3)
          }
        }

      func match {
        case `MakeArray` => lift(Arity1(HasWorkflow).map(makeArray))
        case `MakeObject` =>
          lift(Arity2(HasText, HasWorkflow).map {
            case (name, wf) => makeObject(wf, name)
          })
        case `ObjectConcat` =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((objectConcat(_, _)).tupled)
        case `ArrayConcat` =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((arrayConcat(_, _)).tupled)
        case `Filter` =>
          args match {
            case a1 :: a2 :: Nil =>
              lift(HasWorkflow(a1).flatMap(wf =>
                HasSelector(a2).flatMap(s =>
                  s._2.map(_(a2._2)).sequence.map(filter(wf, _, s._1))) <+>
                  HasJs(a2).flatMap(js =>
                    // TODO: have this pass the JS args as the list of inputs … but right now, those inputs get converted to BsonFields, not ExprOps.
                    js._2.map(_(a2._2)).sequence.map(args => filter(wf, Nil, { case Nil => Selector.Where(js._1(args.map(κ(JsFn.identity)))(JsCore.Ident("this").fix).toJs) })))))
            case _ => fail(FuncArity(func, args.length))
          }
        case `Drop` =>
          lift(Arity2(HasWorkflow, HasInt64).map((skip(_, _)).tupled))
        case `Take` =>
          lift(Arity2(HasWorkflow, HasInt64).map((limit(_, _)).tupled))
        case `Cross` =>
          lift(Arity2(HasWorkflow, HasWorkflow)).flatMap((cross(_, _)).tupled)
        case `GroupBy` =>
          lift(Arity2(HasWorkflow, HasKeys).map((groupBy(_, _)).tupled))
        case `OrderBy` =>
          lift(Arity3(HasWorkflow, HasKeys, HasSortDirs).map {
            case (p1, p2, dirs) => sortBy(p1, p2, dirs)
          })

        case `Constantly` => expr2((v, s) => v)

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

        case `IsNull`     =>
          lift(Arity1(HasWorkflow)).flatMap(
            mapExpr(_)(ExprOp.Eq(_, ExprOp.Literal(Bson.Null))))

        case `Coalesce`   => expr2(ExprOp.IfNull.apply _)

        case `Concat`     => expr2(ExprOp.Concat(_, _, Nil))
        case `Lower`      => expr1(ExprOp.ToLower.apply _)
        case `Upper`      => expr1(ExprOp.ToUpper.apply _)
        case `Substring`  => expr3(ExprOp.Substr(_, _, _))

        case `Cond`       => expr3(ExprOp.Cond.apply _)

        case `Count`      => groupExpr1(κ(ExprOp.Sum(ExprOp.Literal(Bson.Int32(1)))))
        case `Sum`        => groupExpr1(ExprOp.Sum.apply _)
        case `Avg`        => groupExpr1(ExprOp.Avg.apply _)
        case `Min`        => groupExpr1(ExprOp.Min.apply _)
        case `Max`        => groupExpr1(ExprOp.Max.apply _)

        case `Or`         => expr2((a, b) => ExprOp.Or(NonEmptyList.nel(a, b :: Nil)))
        case `And`        => expr2((a, b) => ExprOp.And(NonEmptyList.nel(a, b :: Nil)))
        case `Not`        => expr1(ExprOp.Not.apply)

        case `ArrayLength` =>
          lift(Arity2(HasWorkflow, HasInt64)).flatMap {
            case (p, 1)   => mapExpr(p)(ExprOp.Size(_))
            case (_, dim) => fail(FuncApply(func, "lower array dimension", dim.toString))
          }

        case `Extract`   =>
          lift(Arity2(HasText, HasWorkflow)).flatMap {
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

        case `TimeOfDay`    => {
          def pad2(x: Term[JsCore]) =
            JsCore.Let(JsCore.Ident("x"), x,
              JsCore.If(
                JsCore.BinOp(JsCore.Lt, JsCore.Ident("x").fix, JsCore.Literal(Js.Num(10, false)).fix).fix,
                JsCore.BinOp(JsCore.Add, JsCore.Literal(Js.Str("0")).fix, JsCore.Ident("x").fix).fix,
                JsCore.Ident("x").fix).fix).fix
          def pad3(x: Term[JsCore]) =
            JsCore.Let(JsCore.Ident("x"), x,
              JsCore.If(
                JsCore.BinOp(JsCore.Lt, JsCore.Ident("x").fix, JsCore.Literal(Js.Num(100, false)).fix).fix,
                JsCore.BinOp(JsCore.Add, JsCore.Literal(Js.Str("00")).fix, JsCore.Ident("x").fix).fix,
                JsCore.If(
                  JsCore.BinOp(JsCore.Lt, JsCore.Ident("x").fix, JsCore.Literal(Js.Num(10, false)).fix).fix,
                  JsCore.BinOp(JsCore.Add, JsCore.Literal(Js.Str("0")).fix, JsCore.Ident("x").fix).fix,
                  JsCore.Ident("x").fix).fix).fix).fix
          lift(Arity1(HasWorkflow).flatMap(wb => jsExpr1(wb, JsFn(JsFn.base,
            JsCore.Let(JsCore.Ident("t"), JsFn.base.fix,
              JsCore.BinOp(JsCore.Add,
                pad2(JsCore.Call(JsCore.Select(JsCore.Ident("t").fix, "getUTCHours").fix, Nil).fix),
                JsCore.Literal(Js.Str(":")).fix,
                pad2(JsCore.Call(JsCore.Select(JsCore.Ident("t").fix, "getUTCMinutes").fix, Nil).fix),
                JsCore.Literal(Js.Str(":")).fix,
                pad2(JsCore.Call(JsCore.Select(JsCore.Ident("t").fix, "getUTCSeconds").fix, Nil).fix),
                JsCore.Literal(Js.Str(".")).fix,
                pad3(JsCore.Call(JsCore.Select(JsCore.Ident("t").fix, "getUTCMilliseconds").fix, Nil).fix))).fix))))
        }

        case `ToTimestamp` => expr1(ExprOp.Add(ExprOp.Literal(Bson.Date(Instant.ofEpochMilli(0))), _))

        case `ToId`         => lift(args match {
          case a1 :: Nil =>
            HasText(a1).flatMap(str => BsonCodec.fromData(Data.Id(str)).map(WorkflowBuilder.pure)) <+>
              HasWorkflow(a1).flatMap(src => jsExpr1(src, JsFn(JsFn.base, JsCore.Call(JsCore.Ident("ObjectId").fix, List(JsFn.base.fix)).fix)))
          case _ => -\/(FuncArity(func, args.length))
        })

        case `Between`       => expr3((x, l, u) => ExprOp.And(NonEmptyList.nel(ExprOp.Gte(x, l), ExprOp.Lte(x, u) :: Nil)))

        case `ObjectProject` =>
          lift(Arity2(HasWorkflow, HasText).flatMap((projectField(_, _)).tupled))
        case `ArrayProject` =>
          lift(Arity2(HasWorkflow, HasInt64).flatMap {
            case (p, index) => projectIndex(p, index.toInt)
          })
        case `DeleteField`  =>
          lift(Arity2(HasWorkflow, HasText).flatMap((deleteField(_, _)).tupled))
        case `FlattenObject` => lift(Arity1(HasWorkflow)).flatMap(flattenObject)
        case `FlattenArray` => lift(Arity1(HasWorkflow)).flatMap(flattenArray)
        case `Squash`       => lift(Arity1(HasWorkflow).map(squash))
        case `Distinct`     =>
          lift(Arity1(HasWorkflow)).flatMap(p => distinctBy(p, List(p)))
        case `DistinctBy`   =>
          lift(Arity2(HasWorkflow, HasKeys)).flatMap((distinctBy(_, _)).tupled)

        case `Length`       =>
          lift(Arity1(HasWorkflow).flatMap(jsExpr1(_, JsFn(JsFn.base, JsCore.Select(JsFn.base.fix, "length").fix))))

        case `Search`       => lift(Arity2(HasWorkflow, HasWorkflow)).flatMap {
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
    // need the fold’s Monad to be State[..., \/], so that the morphism
    // flatMaps over the State but not the \/. That way it can evaluate to left
    // for an individual node without failing the fold. This code takes care of
    // mapping from one to the other.
    _ match {
      case ReadF(path) =>
        state(Collection.fromPath(path).map(WorkflowBuilder.read))
      case ConstantF(data) =>
        state(BsonCodec.fromData(data).bimap(
          _ => PlannerError.NonRepresentableData(data),
          WorkflowBuilder.pure))
      case JoinF(left, right, tpe, comp, leftKey, rightKey) =>
        val rez =
          lift((HasWorkflow(left) |@|
            HasWorkflow(right) |@|
            HasWorkflow(leftKey) |@| HasJs(leftKey) |@|
            HasWorkflow(rightKey) |@| HasJs(rightKey))((l, r, lk, lj, rk, rj) =>
            lift((lj._2.map(_(leftKey._2)).sequenceU |@| rj._2.map(_(rightKey._2)).sequenceU)((largs, rargs) =>
              join(l, r, tpe, comp, lk, lj._1(largs.map(κ(JsFn.identity))), rk, rj._1(rargs.map(κ(JsFn.identity)))))).join)).join
        State(s => rez.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
      case InvokeF(func, args) =>
        val v = invoke(func, args)
        State(s => v.run(s).fold(e => s -> -\/(e), t => t._1 -> \/-(t._2)))
      case FreeF(name) =>
        state(-\/(InternalError("variable " + name + " is unbound")))
      case LetF(_, _, in) => state(in._2.head)
    }
  }

  import Planner._

  def plan(logical: Term[LogicalPlan]): EitherWriter[Crystallized] = {
    val annotateƒ = zipPara(
      selectorƒ[OutputM[WorkflowBuilder]],
      liftPara(jsExprƒ[OutputM[WorkflowBuilder]]))

    def log[A](label: String)(a: A)(implicit RA: RenderTree[A]) =
      Planner.emit(Vector(PhaseResult.Tree(label, RA.render(a))), \/-(()))

    for {
      prepared <- withTree("Logical Plan (projections preferred)")(\/-(Optimizer.preferProjections(logical)))

      wst = for {
        wb  <- swapM(lpParaZygoHistoS(prepared)(annotateƒ, workflowƒ))
        wf1 <- build(wb)
      } yield (wb, wf1)
      t <- Planner.emit(Vector.empty, wst.evalZero)
      (wb, wf1) = t
      _   <- log("Workflow Builder")(wb)
      _   <- log("Workflow (raw)")(wf1)

      wf2 =  finish(wf1)
      _   <- log("Workflow (finished)")(wf2)

      wf3 =  crystallize(wf2)
    } yield wf3
  }
}
