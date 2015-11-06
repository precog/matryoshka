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
import quasar.recursionschemes._, Recursive.ops._
import quasar.fp._
import quasar._, Planner._
import quasar.javascript._
import quasar.jscore, jscore.{JsCore, JsFn}

import org.threeten.bp.Instant
import scalaz._, Scalaz._

package object expression {
  /** Expression that can evaluated in the aggregation pipeline */
  type Expression = Fix[ExprOp]

  def $field(field: String, others: String*): Expression =
    $var(DocField(others.map(BsonField.Name).foldLeft[BsonField](BsonField.Name(field))(_ \ _)))

  val $$ROOT = $var(DocVar.ROOT())
  val $$CURRENT = $var(DocVar.CURRENT())

  def bsonDoc(op: String, rhs: Bson) = Bson.Doc(ListMap(op -> rhs))
  private def bsonArr(op: String, elems: Bson*) =
    bsonDoc(op, Bson.Arr(elems.toList))

  val simplifyƒ: ExprOp[Expression] => Option[Expression] = {
    case $condF($literal(Bson.Bool(true)),  c, _) => c.some
    case $condF($literal(Bson.Bool(false)), _, a) => a.some
    case $condF($literal(_),                _, _) => $literal(Bson.Null).some
    case $ifNullF($literal(Bson.Null), r) => r.some
    case $ifNullF($literal(e),         _) => $literal(e).some
    case $notF($literal(Bson.Bool(b))) => $literal(Bson.Bool(!b)).some
    case $notF($literal(_))            => $literal(Bson.Null).some
    case _ => None
  }

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
      case x        => Fix(x)
    }

  implicit val ExprOpTraverse: Traverse[ExprOp] = new Traverse[ExprOp] {
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

  implicit val ExprOpRenderTree: RenderTree[Expression] =
    RenderTree.fromToString[Expression]("ExprOp")

  /** "Literal" translation to JS. */
  def toJsSimpleƒ(expr: ExprOp[JsFn]): PlannerError \/ JsFn = {
    def expr1(x1: JsFn)(f: JsCore => JsCore): PlannerError \/ JsFn =
      \/-(JsFn(JsFn.defaultName, f(x1(jscore.Ident(JsFn.defaultName)))))
    def expr2(x1: JsFn, x2: JsFn)(f: (JsCore, JsCore) => JsCore): PlannerError \/ JsFn =
      \/-(JsFn(JsFn.defaultName, f(x1(jscore.Ident(JsFn.defaultName)), x2(jscore.Ident(JsFn.defaultName)))))

    def unop(op: jscore.UnaryOperator, x: JsFn) =
      expr1(x)(x => jscore.UnOp(op, x))
    def binop(op: jscore.BinaryOperator, l: JsFn, r: JsFn) =
      expr2(l, r)((l, r) => jscore.BinOp(op, l, r))
    def invoke(x: JsFn, name: String) =
      expr1(x)(x => jscore.Call(jscore.Select(x, name), Nil))

    def const(bson: Bson): PlannerError \/ JsCore = {
      def js(l: Js.Lit) = \/-(jscore.Literal(l))
      bson match {
        case Bson.Int64(n)        => js(Js.Num(n, false))
        case Bson.Int32(n)        => js(Js.Num(n, false))
        case Bson.Dec(x)          => js(Js.Num(x, true))
        case Bson.Bool(v)         => js(Js.Bool(v))
        case Bson.Text(v)         => js(Js.Str(v))
        case Bson.Null            => js(Js.Null)
        case Bson.Doc(values)     => values.map { case (k, v) => jscore.Name(k) -> const(v) }.sequenceU.map(jscore.Obj(_))
        case Bson.Arr(values)     => values.toList.map(const(_)).sequenceU.map(jscore.Arr(_))
        case o @ Bson.ObjectId(_) => \/-(JsConversions.jsObjectId(o))
        case d @ Bson.Date(_)     => \/-(JsConversions.jsDate(d))
        // TODO: implement the rest of these (see SD-451)
        case Bson.Regex(_, _)     => -\/(UnsupportedJS(bson.toString))
        case Bson.Symbol(_)       => -\/(UnsupportedJS(bson.toString))
        case Bson.Undefined       => \/-(jscore.ident("undefined"))

        case _ => -\/(NonRepresentableInJS(bson.toString))
      }
    }

    expr match {
      case $includeF()             => -\/(NonRepresentableInJS(expr.toString))
      case $varF(dv)               => \/-(dv.toJs)
      case $addF(l, r)             => binop(jscore.Add, l, r)
      case $andF(f, s, o @ _*)     =>
        \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
          JsFn(JsFn.defaultName, jscore.BinOp(jscore.And, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))
      case $condF(t, c, a)         =>
        \/-(JsFn(JsFn.defaultName,
            jscore.If(t(jscore.Ident(JsFn.defaultName)), c(jscore.Ident(JsFn.defaultName)), a(jscore.Ident(JsFn.defaultName)))))
      case $divideF(l, r)          => binop(jscore.Div, l, r)
      case $eqF(l, r)              => binop(jscore.Eq, l, r)
      case $gtF(l, r)              => binop(jscore.Gt, l, r)
      case $gteF(l, r)             => binop(jscore.Gte, l, r)
      case $literalF(bson)         => const(bson).map(l => JsFn.const(l))
      case $ltF(l, r)              => binop(jscore.Lt, l, r)
      case $lteF(l, r)             => binop(jscore.Lte, l, r)
      case $metaF()                => -\/(NonRepresentableInJS(expr.toString))
      case $multiplyF(l, r)        => binop(jscore.Mult, l, r)
      case $neqF(l, r)             => binop(jscore.Neq, l, r)
      case $notF(a)                => unop(jscore.Not, a)
      case $orF(f, s, o @ _*)      =>
        \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
          JsFn(JsFn.defaultName, jscore.BinOp(jscore.Or, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))

      case $concatF(f, s, o @ _*)  =>
        \/-(NonEmptyList(f, s +: o: _*).foldLeft1((l, r) =>
          JsFn(JsFn.defaultName, jscore.BinOp(jscore.Add, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))
      case $substrF(f, start, len) =>
        \/-(JsFn(JsFn.defaultName,
          jscore.Call(
            jscore.Select(f(jscore.Ident(JsFn.defaultName)), "substr"),
            List(start(jscore.Ident(JsFn.defaultName)), len(jscore.Ident(JsFn.defaultName))))))
      case $subtractF(l, r)        => binop(jscore.Sub, l, r)
      case $toLowerF(a)            => invoke(a, "toLowerCase")
      case $toUpperF(a)            => invoke(a, "toUpperCase")

      case $hourF(a)               => invoke(a, "getUTCHours")
      case $minuteF(a)             => invoke(a, "getUTCMinutes")
      case $secondF(a)             => invoke(a, "getUTCSeconds")
      case $millisecondF(a)        => invoke(a, "getUTCMilliseconds")

      // TODO: implement the rest of these and remove the catch-all (see SD-451)
      case _                       => -\/(UnsupportedJS(expr.toString))
    }
  }

  // The following few cases are places where the ExprOp created from
  // the LogicalPlan needs special handling to behave the same when
  // converted to JS.
  // TODO: See SD-736 for the way forward.
  private val translate: PartialFunction[Expression, PlannerError \/ JsFn] = {
    // matches the pattern the planner generates for converting epoch time
    // values to timestamps. Adding numbers to dates works in ExprOp, but not
    // in Javacript.
    case $add($literal(Bson.Date(inst)), r) if inst.toEpochMilli == 0 =>
      toJs(r).map(r => JsFn(JsFn.defaultName, jscore.New(jscore.Name("Date"), List(r(jscore.Ident(JsFn.defaultName))))))
    // typechecking in ExprOp involves abusing total ordering. This ordering
    // doesn’t hold in JS, so we need to convert back to a typecheck. This
    // checks for a (non-array) object.
    case $and(f, s, o @ _*) => (f, s, o) match {
      // MinKey
      // Null
      case (
        $lt($literal(Bson.Null), f1),
        $lt(f2, $literal(Bson.Text(""))),
        Nil)
          if f1 == f2 =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.isAnyNumber(f(jscore.Ident(JsFn.defaultName)))))
      case (
        $lte($literal(Bson.Text("")), f1),
        $lt(f2, $literal(Bson.Doc(m1))),
        Nil)
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.Call(jscore.ident("isString"), List(f(jscore.Ident(JsFn.defaultName))))))
      case (
        $lte($literal(Bson.Doc(m1)), f1),
        $lt(f2, $literal(b1)),
        Nil)
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]()) =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.Call(jscore.ident("isObject"), List(f(jscore.Ident(JsFn.defaultName))))))
      case (
        $lte($literal(Bson.Doc(m1)), f1),
        $lt(f2, $literal(Bson.Arr(Nil))),
        Nil)
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.And,
            jscore.Call(jscore.ident("isObject"), List(f(jscore.Ident(JsFn.defaultName)))),
            jscore.UnOp(jscore.Not,
              jscore.Call(jscore.Select(jscore.ident("Array"), "isArray"), List(f(jscore.Ident(JsFn.defaultName))))))))
      case (
        $lte($literal(Bson.Arr(Nil)), f1),
        $lt(f2, $literal(b1)),
        Nil)
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]()) =>
        toJs(f1).map(f =>
          JsFn(JsFn.defaultName,
            jscore.Call(jscore.Select(jscore.ident("Array"), "isArray"), List(f(jscore.Ident(JsFn.defaultName))))))
      case (
        $lte($literal(b1), f1),
        $lt(f2, $literal(Bson.ObjectId(oid))),
        Nil)
          if f1 == f2 && b1 == Bson.Binary(scala.Array[Byte]()) && oid == scala.Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Binary"))))
      case (
        $lte($literal(Bson.ObjectId(oid)), f1),
        $lt(f2, $literal(Bson.Bool(false))),
        Nil)
          if f1 == f2 && oid == scala.Array[Byte](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("ObjectId"))))
      case (
        $lte($literal(Bson.Bool(false)), f1),
        $lte(f2, $literal(Bson.Bool(true))),
        Nil)
          if f1 == f2 =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Eq, jscore.UnOp(jscore.TypeOf, f(jscore.Ident(JsFn.defaultName))), jscore.Literal(Js.Str("boolean")))))
      case (
        $lte($literal(Bson.Date(i1)), f1),
        $lt(f2, $literal(Bson.Timestamp(i2, 0))),
        Nil)
          if f1 == f2 && i1 == Instant.ofEpochMilli(0) && i2  == Instant.ofEpochMilli(0)
          =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Date"))))
      case (
        $lte($literal(Bson.Timestamp(i, 0)), f1),
        $lt(f2, $literal(Bson.Regex("", ""))),
        Nil)
          if f1 == f2 && i == Instant.ofEpochMilli(0)
          =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Timestamp"))))
      case (
        $lte($literal(Bson.Date(i)), f1),
        $lt(f2, $literal(Bson.Regex("", ""))),
        Nil)
          if f1 == f2 && i == Instant.ofEpochMilli(0)
          =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Or,
            jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Date")),
            jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Timestamp")))))
      case (
        $lte($literal(Bson.Bool(false)), f1),
        $lt(f2, $literal(Bson.Regex("", ""))),
        Nil)
          if f1 == f2 =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Or,
            jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Date")),
            jscore.BinOp(jscore.Or,
              jscore.BinOp(jscore.Instance, f(jscore.Ident(JsFn.defaultName)), jscore.ident("Timestamp")),
              jscore.BinOp(jscore.Eq, jscore.UnOp(jscore.TypeOf, f(jscore.Ident(JsFn.defaultName))), jscore.Literal(Js.Str("boolean")))))))
      case (
        $lt($literal(Bson.Null), f1),
        $lt(f2, $literal(Bson.Doc(m1))),
        Nil)
          if f1 == f2 && m1 == ListMap() =>
        toJs(f1).map(f => JsFn(JsFn.defaultName,
          jscore.BinOp(jscore.Or,
            jscore.isAnyNumber(f(jscore.Ident(JsFn.defaultName))),
            jscore.Call(jscore.ident("isString"), List(f(jscore.Ident(JsFn.defaultName)))))))
      // Regex
      // MaxKey
      case _ =>
        NonEmptyList(f, s +: o: _*).traverse[PlannerError \/ ?, JsFn](toJs).map(v =>
          v.foldLeft1((l, r) => JsFn(JsFn.defaultName, jscore.BinOp(jscore.And, l(jscore.Ident(JsFn.defaultName)), r(jscore.Ident(JsFn.defaultName))))))
    }
  }

  /** "Idiomatic" translation to JS, accounting for patterns needing special
    * handling. */
  def toJsƒ(t: ExprOp[(Fix[ExprOp], PlannerError \/ JsFn)]): PlannerError \/ JsFn = {
    def expr = Fix(t.map(_._1))
    def js = t.map(_._2).sequenceU
    translate.lift(expr).getOrElse(js.flatMap(toJsSimpleƒ))
  }

  def toJs(expr: Expression): PlannerError \/ JsFn =
    expr.para[PlannerError \/ JsFn](toJsƒ)
}
