package slamdata.engine.javascript

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal}
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
  final case object Add extends BinaryOperator("+")
  final case object BitAnd extends BinaryOperator("&")
  final case object BitLShift extends BinaryOperator("<<")
  final case object BitNot extends BinaryOperator("~")
  final case object BitOr  extends BinaryOperator("|")
  final case object BitRShift  extends BinaryOperator(">>")
  final case object BitXor  extends BinaryOperator("^")
  final case object Lt extends BinaryOperator("<")
  final case object Lte extends BinaryOperator("<=")
  final case object Gt extends BinaryOperator(">")
  final case object Gte extends BinaryOperator(">=")
  final case object Eq extends BinaryOperator("===")
  final case object Neq extends BinaryOperator("!==")
  final case object Div extends BinaryOperator("/")
  final case object In extends BinaryOperator("in")
  final case object And extends BinaryOperator("&&")
  final case object Or extends BinaryOperator("||")
  final case object Mod extends BinaryOperator("%")
  final case object Mult extends BinaryOperator("*")
  final case object Sub extends BinaryOperator("-")

  abstract sealed class UnaryOperator(val js: String) extends Operator
  final case object Neg extends UnaryOperator("-")
  final case object Not extends UnaryOperator("!")

  final case class Literal(value: Js.Lit) extends JsCore[Nothing]
  final case class Ident(name: String) extends JsCore[Nothing]

  final case class Access[A](expr: A, key: A) extends JsCore[A]
  final case class Call[A](callee: A, args: List[A]) extends JsCore[A]
  final case class New[A](name: String, args: List[A]) extends JsCore[A]
  final case class If[A](condition: A, consequent: A, alternative: A) extends JsCore[A]
  final case class UnOp[A](op: UnaryOperator, arg: A) extends JsCore[A]
  final case class BinOp[A](op: BinaryOperator, left: A, right: A) extends JsCore[A]
  object BinOp {
    def apply[A](op: BinaryOperator, a1: Term[JsCore], a2: Term[JsCore], a3: Term[JsCore], args: Term[JsCore]*): Term[JsCore] = args.toList match {
      case Nil    => BinOp(op, a1, BinOp(op, a2, a3).fix).fix
      case h :: t => BinOp(op, a1, BinOp(op, a2, a3, h, t: _*)).fix
    }
  }
  // TODO: Cond
  // TODO: Fn?

  final case class Arr[A](values: List[A]) extends JsCore[A]
  final case class Fun[A](params: List[String], body: A) extends JsCore[A]

  // NB: at runtime, JS may not preserve the order of fields, but using
  // ListMap here lets us be explicit about what result we'd like to see.
  final case class Obj[A](values: ListMap[String, A]) extends JsCore[A]

  final case class Let[A](name: Ident, expr: A, body: A) extends JsCore[A]

  final case class SpliceObjects[A](srcs: List[A]) extends JsCore[A]
  final case class SpliceArrays[A](srcs: List[A]) extends JsCore[A]

  def Select(expr: Term[JsCore], name: String): Access[Term[JsCore]] =
    Access(expr, Literal(Js.Str(name)).fix)

  private[javascript] def toUnsafeJs(expr: Term[JsCore]): Js.Expr = expr.simplify.unFix match {
    case Literal(value)      => value
    case Ident(name)         => Js.Ident(name)
    case Access(expr, key)   => smartDeref(toUnsafeJs(expr), toUnsafeJs(key))
    case Call(callee, args)  => Js.Call(toUnsafeJs(callee), args.map(toUnsafeJs))
    case New(name, args)     => Js.New(Js.Call(Js.Ident(name), args.map(toUnsafeJs(_))))
    case If(cond, cons, alt) => Js.Ternary(toUnsafeJs(cond), toUnsafeJs(cons), toUnsafeJs(alt))

    case UnOp(op, arg)       => Js.UnOp(op.js, toUnsafeJs(arg))
    case BinOp(op, left, right) =>
      Js.BinOp(op.js, toUnsafeJs(left), toUnsafeJs(right))
    case Arr(values)         => Js.AnonElem(values.map(toUnsafeJs(_)))
    case Fun(params, body)   =>
      Js.AnonFunDecl(params, List(Js.Return(toUnsafeJs(body))))
    case Obj(values)         =>
      Js.AnonObjDecl(values.toList.map { case (k, v) => k -> toUnsafeJs(v) })

    case Let(name, expr, body) =>
      Js.Let(ListMap(name.name -> toUnsafeJs(expr)), Nil, toUnsafeJs(body))

    case SpliceObjects(_)    => expr.toJs
    case SpliceArrays(_)     => expr.toJs
  }

  val findFunctionsƒ: JsCore[(Term[JsCore], Set[String])] => Set[String] = {
    case Call((Term(Ident(name)), _), args) =>
      Foldable[List].fold(args.map(_._2)) + name
    case js => js.map(_._2).fold
  }

  def copyAllFields(src: Term[JsCore], dst: Term[JsCore]): Js.Stmt = {
    val tmp = Js.Ident("__attr")  // TODO: use properly-generated temp name (see #581)
    Js.ForIn(tmp, src.toJs,
      Js.If(
        Js.Call(Js.Select(src.toJs, "hasOwnProperty"), List(tmp)),
        Js.BinOp("=", Js.Access(dst.toJs, tmp), Js.Access(src.toJs, tmp)),
        None))
  }

  private def whenDefined(expr: Term[JsCore], body: Js.Expr => Js.Expr, default: => Js.Expr):
      Js.Expr = {
    expr.simplify.unFix match {
      case Literal(Js.Null) => default
      case Literal(_)       => body(expr.toJs)
      case Arr(_)       => body(expr.toJs)
      case Fun(_, _)    => body(expr.toJs)
      case Obj(_)       => body(expr.toJs)
      case Access(x, y) =>
        val bod = body(toUnsafeJs(expr))
        val test = Js.BinOp("&&", Js.BinOp("!=", toUnsafeJs(x), Js.Null),
                                  Js.BinOp("!=", toUnsafeJs(expr), Js.Null))
        Js.Ternary(test, bod, default)
      case _      =>
        // NB: expr is duplicated here, which generates redundant code if expr is
        // a function call, for example. See #581.
        val bod = body(toUnsafeJs(expr))
        val test = Js.BinOp("!=", expr.toJs, Js.Null)
        bod match {
          case Js.Ternary(cond, cons, default0) if default0 == default =>
            Js.Ternary(Js.BinOp("&&", test, cond), cons, default)
          case _ =>
            Js.Ternary(test, bod, default)
        }
    }
  }

  private def smartDeref(expr: Js.Expr, key: Js.Expr): Js.Expr =
    key match {
      case Js.Str(name @ Js.SimpleNamePattern()) =>
        Js.Select(expr, name)
      case _ => Js.Access(expr, key)
    }

  // TODO: Remove this once we have actually functionalized everything
  def safeAssign(lhs: Term[JsCore], rhs: => Term[JsCore]): Js.Expr =
    lhs.simplify.unFix match {
      case Access(obj, key) =>
        whenDefined(obj,
          obj => Js.BinOp("=", smartDeref(obj, key.toJs), rhs.toJs),
          Js.Undefined)
      case _ => Js.BinOp("=", lhs.toJs, rhs.toJs)
    }

  // Check the RHS, but assume the LHS is known to be defined:
  def unsafeAssign(lhs: Term[JsCore], rhs: => Term[JsCore]): Js.Expr =
    Js.BinOp("=", toUnsafeJs(lhs), rhs.toJs)

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
        case Let(name, expr, body)  => G.apply2(f(expr), f(body))(Let(name, _, _))
        case SpliceObjects(srcs)    => G.map(srcs.map(f).sequence)(SpliceObjects(_))
        case SpliceArrays(srcs)     => G.map(srcs.map(f).sequence)(SpliceArrays(_))
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
          smartDeref(_, key.toJs),
          Js.Undefined)

      case Call(Term(Access(Term(New(name, args1)), Term(Literal(Js.Str(mName))))), args2)  =>
        // NB: if we are explicitly constructing a value, we presumably know its fields,
        // so no need to check them, but the args may still come from an unreliable source.
        Js.Call(Js.Select(Js.New(Js.Call(Js.Ident(name), args1.map(_.toJs))), mName), args2.map(_.toJs))
      case Call(Term(Access(arr @ Term(Arr(_)), Term(Literal(Js.Str(mName))))), args) =>
        // NB: if we are explicitly constructing a value, we presumably know its fields,
        // so no need to check them.
        Js.Call(Js.Select(arr.toJs, mName), args.map(_.toJs))
      case Call(expr @ Term(Access(_, _)), args) =>
        // NB: check any other access and the callee together.
        whenDefined(expr, Js.Call(_, args.map(_.toJs)), Js.Undefined)
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

      case Let(name, expr, body) =>
        Js.Let(ListMap(name.name -> expr.toJs), Nil, body.toJs)

      case s @ SpliceObjects(srcs)    =>
        val tmp = Ident("__rez")  // TODO: use properly-generated temp name (see #581)
        Js.Let(
          Map(tmp.name -> Js.AnonObjDecl(Nil)),
          srcs.flatMap {
            case Term(Obj(values)) => values.map { case (k, v) => Js.BinOp("=", smartDeref(tmp.fix.toJs, Js.Str(k)), v.toJs) }
            case src => copyAllFields(src, tmp.fix) :: Nil
          },
          tmp.fix.toJs)

        case s @ SpliceArrays(srcs)    =>
          val tmp = Ident("__rez")  // TODO: use properly-generated temp name (see #581)
          val elem = Ident("__elem")  // TODO: use properly-generated temp name (see #581)
          Js.Let(
            Map(tmp.name -> Js.AnonElem(Nil)),
            srcs.flatMap {
              case Term(Arr(values)) => values.map(v => Js.Call(Js.Select(tmp.fix.toJs, "push"), List(v.toJs)))
              case src => List(
                Js.ForIn(Js.Ident(elem.name), src.toJs,
                  Js.If(
                    Js.Call(Js.Select(src.toJs, "hasOwnProperty"), List(elem.fix.toJs)),
                    Js.Call(Js.Select(tmp.fix.toJs, "push"), List(Js.Access(src.toJs, elem.fix.toJs))),
                    None)))
            },
            tmp.fix.toJs)
    }

    def simplify: Term[JsCore] = {
      expr.rewrite(_.unFix match {
        case Access(Term(Obj(values)), Term(Literal(Js.Str(name)))) =>
          values.get(name)
        case If(cond0, Term(If(cond1, cons, alt1)), alt0) if alt0 == alt1 =>
          Some(If(BinOp(And, cond0, cond1).fix, cons, alt0).fix)

        // NB: inline simple names and selects (e.g. `x`, `x.y`, and `x.y.z`)
        case Let(name, expr @ Term(Ident(_)), body) =>
          Some(body.substitute(name.fix, expr))
        case Let(name, expr @ Term(Access(Term(Ident(_)), Term(Literal(Js.Str(_))))), body) =>
          Some(body.substitute(name.fix, expr))
        case Let(name, expr @ Term(Access(Term(Access(Term(Ident(_)), Term(Literal(Js.Str(_))))), Term(Literal(Js.Str(_))))), body) =>
          Some(body.substitute(name.fix, expr))

        // NB: inline object constructors where the body only extracts one field
        case Let(bound, Term(Obj(values)), Term(Access(Term(name), Term(Literal(Js.Str(key))))))
          if bound == name => values.get(key)

        case x => None
      })
    }

    def substitute(oldExpr: Term[JsCore], newExpr: Term[JsCore]): Term[JsCore] = {
      def loop(x: Term[JsCore], inScope: Set[Term[JsCore]]): Term[JsCore] =
        if (x == oldExpr && !(inScope contains x)) newExpr
        else
          x.unFix match {
            case Let(name, expr, body) => Let(name, loop(expr, inScope), loop(body, inScope + name.fix)).fix
            case Fun(params, body)     => Fun(params, loop(body, inScope ++ params.map(Ident(_).fix).toSet)).fix

            case Access(expr, key)     => Access(loop(expr, inScope), loop(key, inScope)).fix
            case Arr(values)           => Arr(values.map(loop(_, inScope))).fix
            case BinOp(op, l, r)       => BinOp(op, loop(l, inScope), loop(r, inScope)).fix
            case Call(callee, args)    => Call(loop(callee, inScope), args.map(loop(_, inScope))).fix
            case id @ Ident(_)         => id.fix
            case If(cond, cons, alt)   => If(loop(cond, inScope), loop(cons, inScope), loop(alt, inScope)).fix
            case lit @ Literal(_)      => lit.fix
            case New(name, args)       => New(name, args.map(loop(_, inScope))).fix
            case Obj(values)           => Obj(values ∘ (x => loop(x, inScope))).fix
            case SpliceArrays(srcs)    => SpliceArrays(srcs.map(loop(_, inScope))).fix
            case SpliceObjects(srcs)   => SpliceObjects(srcs.map(loop(_, inScope))).fix
            case UnOp(op, x)           => UnOp(op, loop(x, inScope)).fix
          }
      loop(expr, Set.empty)
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
      value.map { case (name, bson) => JsCore.unapply(bson).map(name -> _) }.toList.sequenceU.map(pairs => JsCore.Obj(pairs.toListMap).fix)

    case _ => None
  }
}

final case class JsFn(base: JsCore.Ident, expr: Term[JsCore]) {
  def apply(x: Term[JsCore]) = expr.substitute(base.fix, x)

  def >>>(that: JsFn): JsFn =
    if (this == JsFn.identity) that
    else if (that == JsFn.identity) this
    else JsFn(this.base, JsCore.Let(that.base, this.expr, that.expr).fix.simplify)

  override def toString = JsCore.toUnsafeJs(apply(JsCore.Ident("_").fix).simplify).render(0)

  val commonBase = JsCore.Ident("$")
  override def equals(obj: Any) = obj match {
    case that @ JsFn(_, _) => apply(commonBase.fix).simplify == that.apply(commonBase.fix).simplify
    case _ => false
  }
  override def hashCode = apply(commonBase.fix).simplify.hashCode
}
object JsFn {
  val base = JsCore.Ident("__val")

  val identity = {
    JsFn(base, base.fix)
  }

  def const(x: Term[JsCore]) = JsFn(JsCore.Ident("__unused"), x)

  implicit val JsFnRenderTree = new RenderTree[JsFn] {
    def render(v: JsFn) = Terminal(v.toString, List("JsFn"))
  }
}
