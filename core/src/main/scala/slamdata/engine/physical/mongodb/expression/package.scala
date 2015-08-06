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

import slamdata.Predef._
import slamdata.RenderTree
import slamdata.fixplate.Term
import slamdata.fp._
import slamdata.engine._, Planner._
import slamdata.engine.javascript._

import scalaz._, Scalaz._

package object expression {
  type Expression = Term[ExprOp]

  def $field(field: String, others: String*): Expression =
    $var(DocField(others.map(BsonField.Name).foldLeft[BsonField](BsonField.Name(field))(_ \ _)))
  val $$ROOT = $var(DocVar.ROOT())
  val $$CURRENT = $var(DocVar.CURRENT())

  def bsonDoc(op: String, rhs: Bson) = Bson.Doc(ListMap(op -> rhs))
  private def bsonArr(op: String, elems: Bson*) =
    bsonDoc(op, Bson.Arr(elems.toList))

  val bsonƒ: ExprOp[Bson] => Bson = {
    case $includeF() => Bson.Bool(true)
    case $varF(dv) => dv.bson
    case $andF(first, second, others @ _*) =>
      bsonArr("$and", first +: second +: others: _*)
    case $orF(first, second, others @ _*) =>
      bsonArr("$or", first +: second +: others: _*)
    case $notF(value) => bsonDoc("$not", value)
    case $setEqualsF(left, right) => bsonArr("$setEquals", left, right)
    case $setIntersectionF(left, right) =>
      bsonArr("$setIntersection", left, right)
    case $setDifferenceF(left, right) => bsonArr("$setDifference", left, right)
    case $setUnionF(left, right) => bsonArr("$setUnion", left, right)
    case $setIsSubsetF(left, right) => bsonArr("$setIsSubset", left, right)
    case $anyElementTrueF(value) => bsonDoc("$anyElementTrue", value)
    case $allElementsTrueF(value) => bsonDoc("$allElementsTrue", value)
    case $cmpF(left, right) => bsonArr("$cmp", left, right)
    case $eqF(left, right) => bsonArr("$eq", left, right)
    case $gtF(left, right) => bsonArr("$gt", left, right)
    case $gteF(left, right) => bsonArr("$gte", left, right)
    case $ltF(left, right) => bsonArr("$lt", left, right)
    case $lteF(left, right) => bsonArr("$lte", left, right)
    case $neqF(left, right) => bsonArr("$ne", left, right)
    case $addF(left, right) => bsonArr("$add", left, right)
    case $divideF(left, right) => bsonArr("$divide", left, right)
    case $modF(left, right) => bsonArr("$mod", left, right)
    case $multiplyF(left, right) => bsonArr("$multiply", left, right)
    case $subtractF(left, right) => bsonArr("$subtract", left, right)
    case $concatF(first, second, others @ _*) =>
      bsonArr("$concat", first +: second +: others: _*)
    case $strcasecmpF(left, right) => bsonArr("$strcasecmp", left, right)
    case $substrF(value, start, count) =>
      bsonArr("$substr", value, start, count)
    case $toLowerF(value) => bsonDoc("$toLower", value)
    case $toUpperF(value) => bsonDoc("$toUpper", value)
    case $metaF() => bsonDoc("$meta", Bson.Text("textScore"))
    case $sizeF(array) => bsonDoc("$size", array)
    case $arrayMapF(input, as, in) =>
      bsonDoc(
        "$map",
        Bson.Doc(ListMap(
          "input" -> input,
          "as"    -> Bson.Text(as.name),
          "in"    -> in)))
    case $letF(vars, in) =>
      bsonDoc(
        "$let",
        Bson.Doc(ListMap(
          "vars" -> Bson.Doc(vars.map(t => (t._1.name, t._2))),
          "in"   -> in)))
    case $literalF(value) => bsonDoc("$literal", value)
    case $dayOfYearF(date) => bsonDoc("$dayOfYear", date)
    case $dayOfMonthF(date) => bsonDoc("$dayOfMonth", date)
    case $dayOfWeekF(date) => bsonDoc("$dayOfWeek", date)
    case $yearF(date) => bsonDoc("$year", date)
    case $monthF(date) => bsonDoc("$month", date)
    case $weekF(date) => bsonDoc("$week", date)
    case $hourF(date) => bsonDoc("$hour", date)
    case $minuteF(date) => bsonDoc("$minute", date)
    case $secondF(date) => bsonDoc("$second", date)
    case $millisecondF(date) => bsonDoc("$millisecond", date)
    case $condF(predicate, ifTrue, ifFalse) =>
      bsonArr("$cond", predicate, ifTrue, ifFalse)
    case $ifNullF(expr, replacement) => bsonArr("$ifNull", expr, replacement)
  }

  def rewriteExprRefs(t: Expression)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.cata[Expression] {
      case $varF(f) => $var(applyVar.lift(f).getOrElse(f))
      case x        => Term(x)
    }

  implicit val ExprOpTraverse = new Traverse[ExprOp] {
    def traverseImpl[G[_], A, B](fa: ExprOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[ExprOp[B]] =
      fa match {
        case $includeF()          => G.point($includeF())
        case $varF(dv)            => G.point($varF(dv))
        case $addF(l, r)          => (f(l) |@| f(r))($addF(_, _))
        case $andF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($andF(_, _, _: _*))
        case $setEqualsF(l, r)       => (f(l) |@| f(r))($setEqualsF(_, _))
        case $setIntersectionF(l, r) => (f(l) |@| f(r))($setIntersectionF(_, _))
        case $setDifferenceF(l, r)   => (f(l) |@| f(r))($setDifferenceF(_, _))
        case $setUnionF(l, r)        => (f(l) |@| f(r))($setUnionF(_, _))
        case $setIsSubsetF(l, r)     => (f(l) |@| f(r))($setIsSubsetF(_, _))
        case $anyElementTrueF(v)     => G.map(f(v))($anyElementTrueF(_))
        case $allElementsTrueF(v)    => G.map(f(v))($allElementsTrueF(_))
        case $arrayMapF(a, b, c)  => (f(a) |@| f(c))($arrayMapF(_, b, _))
        case $cmpF(l, r)          => (f(l) |@| f(r))($cmpF(_, _))
        case $concatF(a, b, cs @ _*) => (f(a) |@| f(b) |@| cs.toList.traverse(f))($concatF(_, _, _: _*))
        case $condF(a, b, c)      => (f(a) |@| f(b) |@| f(c))($condF(_, _, _))
        case $dayOfMonthF(a)      => G.map(f(a))($dayOfMonthF(_))
        case $dayOfWeekF(a)       => G.map(f(a))($dayOfWeekF(_))
        case $dayOfYearF(a)       => G.map(f(a))($dayOfYearF(_))
        case $divideF(a, b)       => (f(a) |@| f(b))($divideF(_, _))
        case $eqF(a, b)           => (f(a) |@| f(b))($eqF(_, _))
        case $gtF(a, b)           => (f(a) |@| f(b))($gtF(_, _))
        case $gteF(a, b)          => (f(a) |@| f(b))($gteF(_, _))
        case $hourF(a)            => G.map(f(a))($hourF(_))
        case $metaF()             => G.point($metaF())
        case $sizeF(a)            => G.map(f(a))($sizeF(_))
        case $ifNullF(a, b)       => (f(a) |@| f(b))($ifNullF(_, _))
        case $letF(a, b)          =>
          (Traverse[ListMap[DocVar.Name, ?]].sequence[G, B](a.map(t => t._1 -> f(t._2))) |@| f(b))($letF(_, _))
        case $literalF(lit)       => G.point($literalF(lit))
        case $ltF(a, b)           => (f(a) |@| f(b))($ltF(_, _))
        case $lteF(a, b)          => (f(a) |@| f(b))($lteF(_, _))
        case $millisecondF(a)     => G.map(f(a))($millisecondF(_))
        case $minuteF(a)          => G.map(f(a))($minuteF(_))
        case $modF(a, b)          => (f(a) |@| f(b))($modF(_, _))
        case $monthF(a)           => G.map(f(a))($monthF(_))
        case $multiplyF(a, b)     => (f(a) |@| f(b))($multiplyF(_, _))
        case $neqF(a, b)          => (f(a) |@| f(b))($neqF(_, _))
        case $notF(a)             => G.map(f(a))($notF(_))
        case $orF(a, b, cs @ _*)  => (f(a) |@| f(b) |@| cs.toList.traverse(f))($orF(_, _, _: _*))
        case $secondF(a)          => G.map(f(a))($secondF(_))
        case $strcasecmpF(a, b)   => (f(a) |@| f(b))($strcasecmpF(_, _))
        case $substrF(a, b, c)    => (f(a) |@| f(b) |@| f(c))($substrF(_, _, _))
        case $subtractF(a, b)     => (f(a) |@| f(b))($subtractF(_, _))
        case $toLowerF(a)         => G.map(f(a))($toLowerF(_))
        case $toUpperF(a)         => G.map(f(a))($toUpperF(_))
        case $weekF(a)            => G.map(f(a))($weekF(_))
        case $yearF(a)            => G.map(f(a))($yearF(_))
      }
  }

  implicit val ExprOpRenderTree = RenderTree.fromToString[Expression]("ExprOp")

  // TODO: rewrite as a fold (probably a histo)

  def toJs(expr: Expression): PlannerError \/ JsFn = {
    def expr1(x1: Expression)(f: Term[JsCore] => Term[JsCore]): PlannerError \/ JsFn =
      toJs(x1).map(x1 => JsFn(JsFn.base, f(x1(JsFn.base.fix))))
    def expr2(x1: Expression, x2: Expression)(f: (Term[JsCore], Term[JsCore]) => Term[JsCore]): PlannerError \/ JsFn =
      (toJs(x1) |@| toJs(x2))((x1, x2) => JsFn(JsFn.base, f(x1(JsFn.base.fix), x2(JsFn.base.fix))))

    def unop(op: JsCore.UnaryOperator, x: Expression) =
      expr1(x)(x => JsCore.UnOp(op, x).fix)
    def binop(op: JsCore.BinaryOperator, l: Expression, r: Expression) =
      expr2(l, r)((l, r) => JsCore.BinOp(op, l, r).fix)
    def invoke(x: Expression, name: String) =
      expr1(x)(x => JsCore.Call(JsCore.Select(x, name).fix, Nil).fix)

    def const(bson: Bson): PlannerError \/ Term[JsCore] = {
      def js(l: Js.Lit) = \/-(JsCore.Literal(l).fix)
      bson match {
        case Bson.Int64(n)        => js(Js.Num(n, false))
        case Bson.Int32(n)        => js(Js.Num(n, false))
        case Bson.Dec(x)          => js(Js.Num(x, true))
        case Bson.Bool(v)         => js(Js.Bool(v))
        case Bson.Text(v)         => js(Js.Str(v))
        case Bson.Null            => js(Js.Null)
        case Bson.Doc(values)     => values.map { case (k, v) => k -> const(v) }.sequenceU.map(JsCore.Obj(_).fix)
        case Bson.Arr(values)     => values.toList.map(const(_)).sequenceU.map(JsCore.Arr(_).fix)
        case o @ Bson.ObjectId(_) => \/-(Conversions.jsObjectId(o))
        case d @ Bson.Date(_)     => \/-(Conversions.jsDate(d))
        // TODO: implement the rest of these (see #449)
        case Bson.Regex(pattern)  => -\/(UnsupportedJS(bson.toString))
        case Bson.Symbol(value)   => -\/(UnsupportedJS(bson.toString))

        case _ => -\/(NonRepresentableInJS(bson.toString))
      }
    }

    expr.unFix match {
      // TODO: The following few cases are places where the ExprOp created from
      //       the LogicalPlan needs special handling to behave the same when
      //       converted to JS. See #734 for the way forward.

      // matches the pattern the planner generates for converting epoch time
      // values to timestamps. Adding numbers to dates works in ExprOp, but not
      // in Javacript.
      case $addF($literal(Bson.Date(inst)), r) if inst.toEpochMilli == 0 =>
        expr1(r)(x => JsCore.New("Date", List(x)).fix)
      // typechecking in ExprOp involves abusing total ordering. This ordering
      // doesn’t hold in JS, so we need to convert back to a typecheck. This
      // checks for a (non-array) object.
      case $andF(
        $lte($literal(Bson.Doc(m1)), f1),
        $lt(f2, $literal(Bson.Arr(List()))))
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.And,
              JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Object").fix).fix,
              JsCore.UnOp(JsCore.Not, JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix).fix).fix))
      // same as above, but for arrays
      case $andF(
        $lte($literal(Bson.Arr(List())), f1),
        $lt(f2, $literal(b1)))
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]())=>
        toJs(f1).map(f =>
          JsFn(JsFn.base,
            JsCore.BinOp(JsCore.Instance, f(JsFn.base.fix), JsCore.Ident("Array").fix).fix))

      case $includeF()             => -\/(NonRepresentableInJS(expr.toString))
      case $varF(dv)               => \/-(dv.toJs)
      case $addF(l, r)             => binop(JsCore.Add, l, r)
      case $andF(f, s, o @ _*)     =>
        NonEmptyList(f, s +: o: _*).traverse[PlannerError \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.base, JsCore.BinOp(JsCore.And, l(JsFn.base.fix), r(JsFn.base.fix)).fix)))
      case $condF(t, c, a)         =>
        (toJs(t) |@| toJs(c) |@| toJs(a))((t, c, a) =>
          JsFn(JsFn.base,
            JsCore.If(t(JsFn.base.fix), c(JsFn.base.fix), a(JsFn.base.fix)).fix))
      case $divideF(l, r)          => binop(JsCore.Div, l, r)
      case $eqF(l, r)              => binop(JsCore.Eq, l, r)
      case $gtF(l, r)              => binop(JsCore.Gt, l, r)
      case $gteF(l, r)             => binop(JsCore.Gte, l, r)
      case $literalF(bson)         => const(bson).map(l => JsFn.const(l))
      case $ltF(l, r)              => binop(JsCore.Lt, l, r)
      case $lteF(l, r)             => binop(JsCore.Lte, l, r)
      case $metaF()                => -\/(NonRepresentableInJS(expr.toString))
      case $multiplyF(l, r)        => binop(JsCore.Mult, l, r)
      case $neqF(l, r)             => binop(JsCore.Neq, l, r)
      case $notF(a)                => unop(JsCore.Not, a)

      case $concatF(f, s, o @ _*)  =>
        NonEmptyList(f, s +: o: _*).traverse[PlannerError \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.base, JsCore.BinOp(JsCore.Add, l(JsFn.base.fix), r(JsFn.base.fix)).fix)))
      case $substrF(f, start, len) =>
        (toJs(f) |@| toJs(start) |@| toJs(len))((f, s, l) =>
          JsFn(JsFn.base,
            JsCore.Call(
              JsCore.Select(f(JsFn.base.fix), "substr").fix,
              List(s(JsFn.base.fix), l(JsFn.base.fix))).fix))
      case $subtractF(l, r)        => binop(JsCore.Sub, l, r)
      case $toLowerF(a)            => invoke(a, "toLowerCase")
      case $toUpperF(a)            => invoke(a, "toUpperCase")

      case $hourF(a)               => invoke(a, "getUTCHours")
      case $minuteF(a)             => invoke(a, "getUTCMinutes")
      case $secondF(a)             => invoke(a, "getUTCSeconds")
      case $millisecondF(a)        => invoke(a, "getUTCMilliseconds")

      // TODO: implement the rest of these and remove the catch-all (see #449)
      case _                       => -\/(UnsupportedJS(expr.toString))
    }
  }
}
