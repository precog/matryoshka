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

package quasar

import quasar.Predef._
import quasar.javascript.Js
import quasar.fp._
import quasar.recursionschemes._, Recursive.ops._

import scalaz._, Scalaz._

package object jscore {
  /**
   * Javascript AST (functionnal subset)
   */
  type JsCore = Fix[JsCoreF]


  def ident(value: String): JsCore = Ident(Name(value))

  def binop(op: BinaryOperator, a1: JsCore, args: JsCore*): JsCore = args.toList match {
    case Nil    => a1
    case h :: t => BinOp(op, a1, binop(op, h, t: _*))
  }

  def obj(values: (String, JsCore)*): JsCore =
    Obj(ListMap(values.map { case (k, v) => Name(k) -> v }: _*))

  // TODO: lower-case
  def Select(expr: JsCore, name: String): JsCore =
    Access(expr, Literal(Js.Str(name)))


  private[jscore] def toUnsafeJs(expr: JsCore): Js.Expr = expr.simplify match {
    case Literal(value)      => value
    case Ident(name)         => Js.Ident(name.value)
    case Access(expr, key)   => smartDeref(toUnsafeJs(expr), toUnsafeJs(key))
    case Call(callee, args)  => Js.Call(toUnsafeJs(callee), args.map(toUnsafeJs))
    case New(name, args)     => Js.New(Js.Call(Ident(name).toJs, args.map(toUnsafeJs(_))))
    case If(cond, cons, alt) => Js.Ternary(toUnsafeJs(cond), toUnsafeJs(cons), toUnsafeJs(alt))

    case UnOp(op, arg)       => Js.UnOp(op.js, toUnsafeJs(arg))
    case BinOp(op, left, right) =>
      Js.BinOp(op.js, toUnsafeJs(left), toUnsafeJs(right))
    case Arr(values)         => Js.AnonElem(values.map(toUnsafeJs(_)))
    case Fun(params, body)   =>
      Js.AnonFunDecl(params.map(_.value), List(Js.Return(toUnsafeJs(body))))
    case Obj(values)         =>
      Js.AnonObjDecl(values.toList.map { case (k, v) => k.value -> toUnsafeJs(v) })

    case Let(name, expr, body) =>
      Js.Let(ListMap(name.value -> toUnsafeJs(expr)), Nil, toUnsafeJs(body))

    case SpliceObjects(_)    => expr.toJs
    case SpliceArrays(_)     => expr.toJs
  }

  val findFunctionsƒ: JsCoreF[(Fix[JsCoreF], Set[String])] => Set[String] = {
    case CallF((Fix(IdentF(Name(name))), _), args) =>
      Foldable[List].fold(args.map(_._2)) + name
    case js => js.map(_._2).fold
  }

  def copyAllFields(src: JsCore, dst: Name): Js.Stmt = {
    val tmp = Js.Ident("__attr")  // TODO: use properly-generated temp name (see #581)
    Js.ForIn(tmp, src.toJs,
      Js.If(
        Js.Call(Js.Select(src.toJs, "hasOwnProperty"), List(tmp)),
        Js.BinOp("=", Js.Access(Ident(dst).toJs, tmp), Js.Access(src.toJs, tmp)),
        None))
  }

  private def whenDefined(expr: JsCore, body: Js.Expr => Js.Expr, default: => Js.Expr):
      Js.Expr = {
    expr.simplify match {
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
  def safeAssign(lhs: JsCore, rhs: => JsCore): Js.Expr =
    lhs.simplify match {
      case Access(obj, key) =>
        whenDefined(obj,
          obj => Js.BinOp("=", smartDeref(obj, key.toJs), rhs.toJs),
          Js.Undefined)
      case _ => Js.BinOp("=", lhs.toJs, rhs.toJs)
    }

  // Check the RHS, but assume the LHS is known to be defined:
  def unsafeAssign(lhs: JsCore, rhs: => JsCore): Js.Expr =
    Js.BinOp("=", toUnsafeJs(lhs), rhs.toJs)

  implicit class JsCoreOps(expr: JsCore) {
    def toJs: Js.Expr = expr.simplify match {
      case Literal(value)      => value
      case Ident(name)         => Js.Ident(name.value)

      case Access(expr, key)   =>
        whenDefined(
          expr,
          smartDeref(_, key.toJs),
          Js.Undefined)

      case Call(Access(New(name, args1), Literal(Js.Str(mName))), args2)  =>
        // NB: if we are explicitly constructing a value, we presumably know its fields,
        // so no need to check them, but the args may still come from an unreliable source.
        Js.Call(Js.Select(Js.New(Js.Call(Ident(name).toJs, args1.map(_.toJs))), mName), args2.map(_.toJs))
      case Call(Access(arr @ Arr(_), Literal(Js.Str(mName))), args) =>
        // NB: if we are explicitly constructing a value, we presumably know its fields,
        // so no need to check them.
        Js.Call(Js.Select(arr.toJs, mName), args.map(_.toJs))
      case Call(expr @ Access(_, _), args) =>
        // NB: check any other access and the callee together.
        whenDefined(expr, Js.Call(_, args.map(_.toJs)), Js.Undefined)
      case Call(callee, args)  => Js.Call(callee.toJs, args.map(_.toJs))

      case New(name, args)     => Js.New(Js.Call(Ident(name).toJs, args.map(_.toJs)))
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
        Js.AnonFunDecl(params.map(_.value), List(Js.Return(body.toJs)))
      case Obj(values)         =>
        Js.AnonObjDecl(values.toList.map { case (k, v) => k.value -> v.toJs })

      case Let(name, expr, body) =>
        Js.Let(ListMap(name.value -> expr.toJs), Nil, body.toJs)

      case s @ SpliceObjects(srcs)    =>
        val tmp = Name("__rez")  // TODO: use properly-generated temp name (see #581)
        Js.Let(
          Map(tmp.value -> Js.AnonObjDecl(Nil)),
          srcs.flatMap {
            case Obj(values) => values.map { case (k, v) => Js.BinOp("=", smartDeref(Ident(tmp).toJs, Js.Str(k.value)), v.toJs) }
            case src => copyAllFields(src, tmp) :: Nil
          },
          Ident(tmp).toJs)

        case s @ SpliceArrays(srcs)    =>
          val tmp = Name("__rez")  // TODO: use properly-generated temp name (see #581)
          val elem = Name("__elem")  // TODO: use properly-generated temp name (see #581)
          Js.Let(
            Map(tmp.value -> Js.AnonElem(Nil)),
            srcs.flatMap {
              case Arr(values) => values.map(v => Js.Call(Js.Select(Ident(tmp).toJs, "push"), List(v.toJs)))
              case src => List(
                Js.ForIn(Js.Ident(elem.value), src.toJs,
                  Js.If(
                    Js.Call(Js.Select(src.toJs, "hasOwnProperty"), List(Ident(elem).toJs)),
                    Js.Call(Js.Select(Ident(tmp).toJs, "push"), List(Js.Access(src.toJs, Ident(elem).toJs))),
                    None)))
            },
            Ident(tmp).toJs)
    }

    def simplify: JsCore = {
      expr.rewrite(_ match {
        case Access(Obj(values), Literal(Js.Str(name))) =>
          values.get(Name(name))
        case If(cond0, If(cond1, cons, alt1), alt0) if alt0 == alt1 =>
          Some(If(BinOp(And, cond0, cond1), cons, alt0))

        // NB: inline simple names and selects (e.g. `x`, `x.y`, and `x.y.z`)
        case Let(name, expr @ Ident(_), body) =>
          Some(body.substitute(Ident(name), expr))
        case Let(name, expr @ Access(Ident(_), Literal(Js.Str(_))), body) =>
          Some(body.substitute(Ident(name), expr))
        case Let(name, expr @ Access(Access(Ident(_), Literal(Js.Str(_))), Literal(Js.Str(_))), body) =>
          Some(body.substitute(Ident(name), expr))

        // NB: inline object constructors where the body only extracts one field
        case Let(bound, Obj(values), Access(name, Literal(Js.Str(key))))
          if bound == name => values.get(Name(key))

        case x => None
      })
    }

    def substitute(oldExpr: JsCore, newExpr: JsCore): JsCore = {
      def loop(x: JsCore, inScope: Set[JsCore]): JsCore =
        if (x == oldExpr && !(inScope contains x)) newExpr
        else
          x match {
            case Let(name, expr, body) => Let(name, loop(expr, inScope), loop(body, inScope + Ident(name)))
            case Fun(params, body)     => Fun(params, loop(body, inScope ++ params.map(Ident(_)).toSet))

            case Access(expr, key)     => Access(loop(expr, inScope), loop(key, inScope))
            case Arr(values)           => Arr(values.map(loop(_, inScope)))
            case BinOp(op, l, r)       => BinOp(op, loop(l, inScope), loop(r, inScope))
            case Call(callee, args)    => Call(loop(callee, inScope), args.map(loop(_, inScope)))
            case id @ Ident(_)         => id
            case If(cond, cons, alt)   => If(loop(cond, inScope), loop(cons, inScope), loop(alt, inScope))
            case lit @ Literal(_)      => lit
            case New(name, args)       => New(name, args.map(loop(_, inScope)))
            case Obj(values)           => Obj(values ∘ (x => loop(x, inScope)))
            case SpliceArrays(srcs)    => SpliceArrays(srcs.map(loop(_, inScope)))
            case SpliceObjects(srcs)   => SpliceObjects(srcs.map(loop(_, inScope)))
            case UnOp(op, x)           => UnOp(op, loop(x, inScope))
          }
      loop(expr, Set.empty)
    }
  }

  implicit val JsCoreTraverse: Traverse[JsCoreF] = new Traverse[JsCoreF] {
    def traverseImpl[G[_], A, B](fa: JsCoreF[A])(f: A => G[B])(implicit G: Applicative[G]): G[JsCoreF[B]] = {
      fa match {
        case LiteralF(lit)           => G.point(LiteralF(lit))
        case IdentF(name)            => G.point(IdentF(name))
        case AccessF(expr, key)      => G.apply2(f(expr), f(key))(AccessF(_, _))
        case CallF(expr, args)       => G.apply2(f(expr), args.map(f).sequence)(CallF(_, _))
        case NewF(name, args)        => G.map(args.map(f).sequence)(NewF(name, _))
        case IfF(cond, cons, alt)    => G.apply3(f(cond), f(cons), f(alt))(IfF(_, _, _))
        case UnOpF(op, arg)          => G.map(f(arg))(UnOpF(op, _))
        case BinOpF(op, left, right) => G.apply2(f(left), f(right))(BinOpF(op, _, _))
        case ArrF(values)            => G.map(values.map(f).sequence)(ArrF(_))
        case FunF(params, body)      => G.map(f(body))(FunF(params, _))
        case ObjF(values)            => G.map((values ∘ f).sequence)(ObjF(_))
        case LetF(name, expr, body)  => G.apply2(f(expr), f(body))(LetF(name, _, _))
        case SpliceObjectsF(srcs)    => G.map(srcs.map(f).sequence)(SpliceObjectsF(_))
        case SpliceArraysF(srcs)     => G.map(srcs.map(f).sequence)(SpliceArraysF(_))
      }
    }
  }

  implicit val JsCoreRenderTree = new RenderTree[JsCore] {
    val nodeType = List("JsCore")

    def simpleƒ(v: JsCoreF[Boolean]): Boolean = v match {
      case IdentF(_)            => true
      case LiteralF(_)          => true

      case ArrF(values)         => values.all(_ == true)
      case AccessF(expr, key)   => expr && key
      case BinOpF(_, l, r)      => l && r
      case CallF(callee, args)  => callee && args.all(_ == true)
      case IfF(cond, cons, alt) => cond && cons && alt
      case LetF(_, expr, body)  => expr && body
      case NewF(_, args)        => args.all(_ == true)
      case UnOpF(_, x)          => x

      case FunF(_, body)        => false
      case ObjF(_)              => false
      case SpliceArraysF(_)     => false
      case SpliceObjectsF(_)    => false
    }

    def renderSimple(v: JsCore): Option[RenderedTree] =
      if (v.cata(simpleƒ)) Some(Terminal(nodeType, Some(toUnsafeJs(v).pprint(0))))
      else None

    def render(v: JsCore) = v match {
      case Ident(name)           => Terminal("Ident" :: nodeType, Some(name.value))
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
        NonTerminal("New" :: nodeType, Some(name.value), args.map(render)))
      case UnOp(op, x)           => renderSimple(v).getOrElse(
        NonTerminal("UnOp" :: nodeType, Some(op.js), List(render(x))))

      case Obj(values)           =>
        NonTerminal("Obj" :: nodeType, None,
          values.toList.map { case (n, v) =>
            if (v.cata(simpleƒ)) Terminal("Key" :: nodeType, Some(n.value + ": " + toUnsafeJs(v).pprint(0)))
            else NonTerminal("Key" :: nodeType, Some(n.value), List(render(v)))
          })
      case SpliceArrays(srcs)    => NonTerminal("SpliceArrays" :: nodeType, None, srcs.map(render))
      case SpliceObjects(srcs)   => NonTerminal("SpliceObjects" :: nodeType, None, srcs.map(render))
      case Let(name, expr, body) => NonTerminal("Let" :: nodeType, Some(name.value), List(render(expr), render(body)))
      case Fun(params, body)     => NonTerminal("Fun" :: nodeType, Some(params.mkString(", ")), List(render(body)))
    }
  }
}
