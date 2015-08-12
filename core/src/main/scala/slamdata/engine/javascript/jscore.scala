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

package slamdata.engine.javascript

import slamdata.Predef._
import slamdata.{RenderTree, Terminal, NonTerminal, RenderedTree}
import slamdata.fixplate._
import slamdata.fp._

import scalaz._; import Scalaz._

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
  final case object Instance extends BinaryOperator("instanceof")

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

  implicit val JsCoreRenderTree = new RenderTree[Term[JsCore]] {
    val nodeType = List("JsCore")

    def simpleƒ(v: JsCore[Boolean]): Boolean = v match {
      case Ident(_)            => true
      case Literal(_)          => true

      case Arr(values)         => values.all(_ == true)
      case Access(expr, key)   => expr && key
      case BinOp(_, l, r)      => l && r
      case Call(callee, args)  => callee && args.all(_ == true)
      case If(cond, cons, alt) => cond && cons && alt
      case Let(_, expr, body)  => expr && body
      case New(_, args)        => args.all(_ == true)
      case UnOp(_, x)          => x

      case Fun(_, body)        => false
      case Obj(_)              => false
      case SpliceArrays(_)     => false
      case SpliceObjects(_)    => false
    }

    def renderSimple(v: Term[JsCore]): Option[RenderedTree] =
      if (v.cata(simpleƒ)) Some(Terminal(nodeType, Some(toUnsafeJs(v).pprint(0))))
      else None

    def render(v: Term[JsCore]) = v.unFix match {
      case Ident(name)           => Terminal("Ident" :: nodeType, Some(name))
      case Literal(js)           => Terminal("Literal" :: nodeType, Some(js.pprint(0)))

      case Arr(values)           => renderSimple(v).getOrElse(
        NonTerminal("Arr" :: nodeType, None, values.map(render)))
      case Access(expr, key)     => renderSimple(v).getOrElse(
        NonTerminal("Access" :: nodeType, None, List(render(expr), render(key))))
      case BinOp(op, l, r)       => renderSimple(v).getOrElse(
        NonTerminal("BinOp" :: nodeType, Some(op.js), List(render(l), render(r))))
      case Call(callee, args)    => renderSimple(v).getOrElse(
        NonTerminal("Call" :: nodeType, None, render(callee) :: args.map(render)))
      case If(cond, cons, alt)   => renderSimple(v).getOrElse(
        NonTerminal("If" :: nodeType, None, List(render(cond), render(cons), render(alt))))
      case New(name, args)       => renderSimple(v).getOrElse(
        NonTerminal("New" :: nodeType, Some(name), args.map(render)))
      case UnOp(op, x)           => renderSimple(v).getOrElse(
        NonTerminal("UnOp" :: nodeType, Some(op.js), List(render(x))))

      case Obj(values)                  =>
        NonTerminal("Obj" :: nodeType, None,
          values.toList.map { case (n, v) =>
            if (v.cata(simpleƒ)) Terminal("Key" :: nodeType, Some(n + ": " + toUnsafeJs(v).pprint(0)))
            else NonTerminal("Key" :: nodeType, Some(n), List(render(v)))
          })
      case SpliceArrays(srcs)           => NonTerminal("SpliceArrays" :: nodeType, None, srcs.map(render))
      case SpliceObjects(srcs)          => NonTerminal("SpliceObjects" :: nodeType, None, srcs.map(render))
      case Let(Ident(name), expr, body) => NonTerminal("Let" :: nodeType, Some(name), List(render(expr), render(body)))
      case Fun(params, body)            => NonTerminal("Fun" :: nodeType, Some(params.mkString(", ")), List(render(body)))
    }
  }
}

final case class JsFn(base: JsCore.Ident, expr: Term[JsCore]) {
  def apply(x: Term[JsCore]) = expr.substitute(base.fix, x)

  def >>>(that: JsFn): JsFn =
    if (this == JsFn.identity) that
    else if (that == JsFn.identity) this
    else JsFn(this.base, JsCore.Let(that.base, this.expr, that.expr).fix.simplify)

  override def toString = JsCore.toUnsafeJs(apply(JsCore.Ident("_").fix).simplify).pprint(0)

  val commonBase = JsCore.Ident("$")
  override def equals(obj: scala.Any) = obj match {
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
    def render(v: JsFn) = RenderTree[Term[JsCore]].render(v(JsCore.Ident("_").fix))
  }
}
