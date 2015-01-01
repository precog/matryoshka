package slamdata.engine.javascript

import scala.collection.immutable.Map

import scalaz._
import Scalaz._

import slamdata.engine.analysis.fixplate._

/**
  ADT for a simplified, composable, core language for JavaScript. Provides only
  expressions, including lets.
  */
sealed trait JsCore[+A]
object JsCore {
  case class Literal(value: Js.Lit) extends JsCore[Nothing]
  case class Ident(name: String) extends JsCore[Nothing]

  case class Access[A](expr: A, key: A) extends JsCore[A]
  case class Call[A](callee: A, args: List[A]) extends JsCore[A]
  case class New[A](name: String, args: List[A]) extends JsCore[A]

  case class UnOp[A](op: String, arg: A) extends JsCore[A]
  case class BinOp[A](op: String, left: A, right: A) extends JsCore[A]

  // TODO: Cond
  // TODO: Fn?

  case class Arr[A](values: List[A]) extends JsCore[A]
  case class Obj[A](values: Map[String, A]) extends JsCore[A]

  case class Let[A](bindings: Map[String, A], expr: A) extends JsCore[A]
  
  def Select(expr: Term[JsCore], name: String): Access[Term[JsCore]] =
    Access(expr, Literal(Js.Str(name)).fix)

  implicit class UnFixedJsCoreOps(expr: JsCore[Term[JsCore]]) {
    def fix = Term[JsCore](expr)
  }
  
  implicit class JsCoreOps(expr: Term[JsCore]) {
    def toJs: Js.Expr = expr.unFix match {
      case Literal(value)      => value
      case Ident(name)         => Js.Ident(name)

      case Access(expr, key)   => Js.safeDeref(expr.toJs, key.toJs)
      case Call(Term(Access(obj, Term(Literal(Js.Str(fn))))), args) =>
        Js.safeCall(obj.toJs, fn, args.map(_.toJs))
      case Call(callee, args)  => Js.Call(callee.toJs, args.map(_.toJs))
      case New(name, args)     => Js.New(Js.Call(Js.Ident(name), args.map(_.toJs)))

      case UnOp(op, arg)       =>
        Js.whenDefined(arg.toJs, Js.UnOp(op, _), Js.Null)
      case BinOp("=", Term(Access(left1, left2)), right) =>
        Js.safeAssign(left1.toJs, left2.toJs, right.toJs)
      case BinOp(op, left, right) =>
        Js.whenDefined(
          left.toJs,
          l => Js.whenDefined(right.toJs, r => Js.BinOp(op, l, r), Js.Null),
          Js.Null)

      case Arr(values)         => Js.AnonElem(values.map(_.toJs))
      case Obj(values)         => Js.AnonObjDecl(values.toList.map { case (k, v) => k -> v.toJs })

      case Let(bindings, expr) =>
        Js.Let(bindings.mapValues(_.toJs), Nil, expr.toJs)
    }
  }

  import slamdata.engine.physical.mongodb.{Bson}

  def unapply(value: Bson): Option[Term[JsCore]] = value match {
    case Bson.Null         => Some(JsCore.Literal(Js.Null).fix)
    case Bson.Text(str)    => Some(JsCore.Literal(Js.Str(str)).fix)
    case Bson.Bool(value)  => Some(JsCore.Literal(Js.Bool(value)).fix)
    case Bson.Int32(value) => Some(JsCore.Literal(Js.Num(value, false)).fix)
    case Bson.Int64(value) => Some(JsCore.Literal(Js.Num(value, false)).fix)
    case Bson.Dec(value)   => Some(JsCore.Literal(Js.Num(value, true)).fix)

    case Bson.Doc(value)     =>
      val a: Option[List[(String, Term[JsCore])]] = value.map { case (name, bson) => JsCore.unapply(bson).map(name -> _) }.toList.sequenceU
      a.map(as => JsCore.Obj(Map(as: _*)).fix)

    case _ => None
  }
}

case class JsMacro(expr: Term[JsCore] => Term[JsCore]) {
  def apply(x: Term[JsCore]) = expr(x)
  
  def >>>(right: JsMacro): JsMacro = JsMacro(x => right.expr(this.expr(x)))
  
  override def toString = expr(JsCore.Ident("_").fix).toJs.render(0)
  
  private val impossibleName = JsCore.Ident("\\").fix
  override def equals(obj: Any) = obj match {
    case JsMacro(expr2) => expr(impossibleName) == expr2(impossibleName)
    case _ => false
  }
  override def hashCode = expr(impossibleName).hashCode
}
