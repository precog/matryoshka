package slamdata.engine.javascript

import scala.collection.immutable.Map

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._

/**
  ADT for a simplified, composable, core language for JavaScript. Provides only
  expressions, including lets.
  */
sealed trait JsCore[+A]
object JsCore {
  sealed trait Operator {
    val js: String
  }
  abstract sealed class BinaryOperator(val js: String) extends Operator
  case object Add extends BinaryOperator("+")
  case object BitAnd extends BinaryOperator("&")
  case object BitLShift extends BinaryOperator("<<")
  case object BitNot extends BinaryOperator("~")
  case object BitOr  extends BinaryOperator("|")
  case object BitRShift  extends BinaryOperator(">>")
  case object BitXor  extends BinaryOperator("^")
  case object Lt extends BinaryOperator("<")
  case object Lte extends BinaryOperator("<=")
  case object Gt extends BinaryOperator(">")
  case object Gte extends BinaryOperator(">=")
  case object Eq extends BinaryOperator("===")
  case object Neq extends BinaryOperator("!==")
  case object Div extends BinaryOperator("/")
  case object In extends BinaryOperator("in")
  case object And extends BinaryOperator("&&")
  case object Or extends BinaryOperator("||")
  case object Mod extends BinaryOperator("%")
  case object Mult extends BinaryOperator("*")
  case object Sub extends BinaryOperator("-")

  abstract sealed class UnaryOperator(val js: String) extends Operator
  case object Neg extends UnaryOperator("-")
  case object Not extends UnaryOperator("!")

  case class Literal(value: Js.Lit) extends JsCore[Nothing]
  case class Ident(name: String) extends JsCore[Nothing]

  case class Access[A](expr: A, key: A) extends JsCore[A]
  case class Call[A](callee: A, args: List[A]) extends JsCore[A]
  case class New[A](name: String, args: List[A]) extends JsCore[A]
  case class If[A](condition: A, consequent: A, alternative: A) extends JsCore[A]
  case class UnOp[A](op: UnaryOperator, arg: A) extends JsCore[A]
  case class BinOp[A](op: BinaryOperator, left: A, right: A) extends JsCore[A]

  // TODO: Cond
  // TODO: Fn?

  case class Arr[A](values: List[A]) extends JsCore[A]
  case class Fun[A](params: List[String], body: A) extends JsCore[A]
  case class Obj[A](values: Map[String, A]) extends JsCore[A]

  case class Let[A](bindings: Map[String, A], expr: A) extends JsCore[A]

  def Select(expr: Term[JsCore], name: String): Access[Term[JsCore]] =
    Access(expr, Literal(Js.Str(name)).fix)

    private def whenDefined(expr: Term[JsCore], body: Js.Expr => Js.Expr, default: => Js.Expr):
        Js.Expr = {
      def toUnsafeJs(expr: Term[JsCore]): Js.Expr = expr.simplify.unFix match {
        case Literal(value)      => value
        case Ident(name)         => Js.Ident(name)
        case Access(expr, key)   => smartDeref(toUnsafeJs(expr), key)
        case Call(callee, args)  => Js.Call(toUnsafeJs(callee), args.map(toUnsafeJs))
        case New(name, args)     => Js.New(Js.Call(Js.Ident(name), args.map(_.toJs)))
        case If(cond, cons, alt) => Js.Ternary(cond.toJs, cons.toJs, alt.toJs)

        case UnOp(op, arg)       => Js.UnOp(op.js, toUnsafeJs(arg))
        case BinOp(op, left, right) =>
          Js.BinOp(op.js, toUnsafeJs(left), toUnsafeJs(right))
        case Arr(values)         => Js.AnonElem(values.map(_.toJs))
        case Fun(params, body)   =>
          Js.AnonFunDecl(params, List(Js.Return(body.toJs)))
        case Obj(values)         =>
          Js.AnonObjDecl(values.toList.map { case (k, v) => k -> v.toJs })

        case Let(bindings, expr) =>
          Js.Let(bindings.mapValues(_.toJs), Nil, expr.toJs)
      }

      expr.unFix match {
        case Literal(Js.Null) => default
        case Literal(_)       => body(expr.toJs)
        case _      =>
          val bod = body(toUnsafeJs(expr))
          bod match {
            case Js.Ternary(cond, cons, default0) if default0 == default =>
              Js.Ternary(Js.BinOp("&&", Js.BinOp("!=", expr.toJs, Js.Null), cond), cons, default)
            case _ =>
              Js.Ternary(Js.BinOp("!=", expr.toJs, Js.Null), bod, default)
          }
      }
    }

  private def smartDeref(expr: Js.Expr, key: Term[JsCore]): Js.Expr =
    key.unFix match {
      case Literal(Js.Str(name @ Js.SimpleNamePattern())) =>
        Js.Select(expr, name)
      case _ => Js.Access(expr, key.toJs)
    }

  // TODO: Remove this once we have actually functionalized everything
  def safeAssign(lhs: Term[JsCore], rhs: => Term[JsCore]): Js.Expr =
    lhs.unFix match {
      case Access(obj, key) =>
        whenDefined(obj,
          obj => Js.BinOp("=", smartDeref(obj, key), rhs.toJs),
          Js.Ident("undefined"))
      case _ => Js.BinOp("=", lhs.toJs, rhs.toJs)
    }

  implicit val JsCoreTraverse: Traverse[JsCore] = new Traverse[JsCore] {
    def traverseImpl[G[_], A, B](fa: JsCore[A])(f: A => G[B])(implicit G: Applicative[G]): G[JsCore[B]] = {
      fa match {
        case x @ Literal(_)         => G.point(x)
        case x @ Ident(_)           => G.point(x)
        case Access(expr, key)      => G.apply2(f(expr), f(key))(Access(_, _))
        case Call(expr, args)       => G.apply2(f(expr), args.map(f).sequence)(Call(_, _))
        case New(name, args)        => G.map(args.map(f).sequence)(New(name, _))
        case If(cond, cons, alt)    => G.apply3(f(cond), f(cons), f(alt))(If(_, _, _))
        case UnOp(op, arg)          => G.map(f(arg))(UnOp(op, _))
        case BinOp(op, left, right) => G.apply2(f(left), f(right))(BinOp(op, _, _))
        case Arr(values)            => G.map(values.map(f).sequence)(Arr(_))
        case Fun(params, body)      => G.map(f(body))(Fun(params, _))
        case Obj(values)            => G.map((values ∘ f).sequence)(Obj(_))
        case Let(bindings, expr)    => G.apply2((bindings ∘ f).sequence, f(expr))(Let(_, _))
      }
    }
  }

  implicit class UnFixedJsCoreOps(expr: JsCore[Term[JsCore]]) {
    def fix = Term[JsCore](expr)
  }

  implicit class JsCoreOps(expr: Term[JsCore]) {
    def toJs: Js.Expr = expr.simplify.unFix match {
      case Literal(value)      => value
      case Ident(name)         => Js.Ident(name)

      case Access(expr, key)   =>
        whenDefined(
          expr,
          smartDeref(_, key),
          Js.Ident("undefined"))
      case Call(Term(Access(obj, key)), args) =>
        whenDefined(
          obj,
          obj => Js.Call(smartDeref(obj, key), args.map(_.toJs)),
          Js.Null)
      case Call(callee, args)  => Js.Call(callee.toJs, args.map(_.toJs))
      case New(name, args)     => Js.New(Js.Call(Js.Ident(name), args.map(_.toJs)))
      case If(cond, cons, alt) => Js.Ternary(cond.toJs, cons.toJs, alt.toJs)

      case UnOp(op, arg)       =>
        whenDefined(arg, Js.UnOp(op.js, _), Js.Null)
      case BinOp(op, left, right) =>
        whenDefined(
          left,
          l => whenDefined(right, r => Js.BinOp(op.js, l, r), Js.Null),
          Js.Null)

      case Arr(values)         => Js.AnonElem(values.map(_.toJs))
      case Fun(params, body)   =>
        Js.AnonFunDecl(params, List(Js.Return(body.toJs)))
      case Obj(values)         =>
        Js.AnonObjDecl(values.toList.map { case (k, v) => k -> v.toJs })

      case Let(bindings, expr) =>
        Js.Let(bindings.mapValues(_.toJs), Nil, expr.toJs)
    }

    def simplify: Term[JsCore] = {
      expr.rewrite(_.unFix match {
        case Access(Term(Obj(values)), Term(Literal(Js.Str(name)))) =>
          values.get(name)
        case If(cond0, Term(If(cond1, cons, alt1)), alt0) if alt0 == alt1 =>
          Some(If(BinOp(And, cond0, cond1).fix, cons, alt0).fix)
        case _ => None
      })
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
  
  override def toString = expr(JsCore.Ident("_").fix).simplify.toJs.render(0)

  private val impossibleName = JsCore.Ident("\\").fix
  override def equals(obj: Any) = obj match {
    case JsMacro(expr2) => expr(impossibleName).simplify == expr2(impossibleName).simplify
    case _ => false
  }
  override def hashCode = expr(impossibleName).simplify.hashCode
}
