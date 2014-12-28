package slamdata.engine.javascript

import scala.collection.immutable.ListMap

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
  case class Obj[A](values: ListMap[String, A]) extends JsCore[A]

  case class Let[A](bindings: ListMap[String, A], expr: A) extends JsCore[A]
  
  def Select(expr: Term[JsCore], name: String): Access[Term[JsCore]] = Access(expr, Literal(Js.Str(name)).fix)

  implicit val JsCoreTraverse: Traverse[JsCore] = new Traverse[JsCore] {
    def traverseImpl[G[_], A, B](fa: JsCore[A])(f: A => G[B])(implicit G: Applicative[G]): G[JsCore[B]] = {
      fa match {
        case x @ Literal(_)         => G.point(x)
        case x @ Ident(_)           => G.point(x)
        case Access(expr, key)      => G.apply2(f(expr), f(key))(Access(_, _))
        case Call(expr, args)       => G.apply2(f(expr), args.map(f).sequence)(Call(_, _))
        case New(name, args)        => G.map(args.map(f).sequence)(New(name, _))
        case UnOp(op, arg)          => G.map(f(arg))(UnOp(op, _))
        case BinOp(op, left, right) => G.apply2(f(left), f(right))(BinOp(op, _, _))
        case Arr(values)            => G.map(values.map(f).sequence)(Arr(_))
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

      case Access(expr, key)   => key.unFix match {
        case Literal(Js.Str(name @ Js.SimpleNamePattern())) => Js.Select(expr.toJs, name)
        case _ => Js.Access(expr.toJs, key.toJs)
      }
      case Call(callee, args)  => Js.Call(callee.toJs, args.map(_.toJs))
      case New(name, args)     => Js.New(Js.Call(Js.Ident(name), args.map(_.toJs)))

      case UnOp(op, arg)       => Js.UnOp(op, arg.toJs)
      case BinOp(op, left, right) => Js.BinOp(op, left.toJs, right.toJs)

      case Arr(values)         => Js.AnonElem(values.map(_.toJs))
      case Obj(values)         => Js.AnonObjDecl(values.toList.map { case (k, v) => k -> v.toJs })

      case Let(bindings, expr) => 
        Js.Call(
          Js.AnonFunDecl(
            bindings.keys.toList, 
            List(Js.Return(expr.toJs))),
          bindings.values.map(_.toJs).toList)
    }

    def simplify: Term[JsCore] = {
      expr.rewrite(_ match {
        case Term(Access(Term(Obj(values)), Term(Literal(Js.Str(name))))) => 
          values.get(name)
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
      a.map(as => JsCore.Obj(ListMap(as: _*)).fix)

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
