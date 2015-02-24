package slamdata.engine.javascript

import org.specs2.mutable._

import slamdata.engine.{TreeMatchers}
import slamdata.engine.analysis.fixplate.Term

import scala.collection.immutable.ListMap

class JsCoreSpecs extends Specification with TreeMatchers {
  import JsCore._

  "toJs" should {
    "handle projecting a value safely" in {
      Access(Ident("foo").fix, Ident("bar").fix).fix.toJs must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.Access(Js.Ident("foo"), Js.Ident("bar")),
        Js.Undefined)
    }

    "prevent projecting from null" in {
      Access(Literal(Js.Null).fix, Ident("bar").fix).fix.toJs must_==
      Js.Undefined
    }

    "not protect projections from literals" in {
      Access(Literal(Js.Num(1, false)).fix, Ident("bar").fix).fix.toJs must_==
      Js.Access(Js.Num(1, false), Js.Ident("bar"))
    }

    "handle calling a projection safely" in {
      val expr = Call(Select(Ident("foo").fix, "bar").fix, Nil).fix
      val exp =
        Js.Ternary(
          Js.BinOp("&&",
            Js.BinOp("!=", Js.Ident("foo"), Js.Null),
            Js.BinOp("!=", Js.Select(Js.Ident("foo"), "bar"), Js.Null)),
          Js.Call(Js.Select(Js.Ident("foo"), "bar"), Nil),
          Js.Undefined)

      expr.toJs.render(0) must_== exp.render(0)
    }

    "handle assigning to a property safely" in {
      safeAssign(Select(Ident("foo").fix, "bar").fix, Ident("baz").fix) must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.BinOp("=", Js.Select(Js.Ident("foo"), "bar"), Js.Ident("baz")),
        Js.Undefined)
    }

    "handle binary operations safely" in {
      BinOp(Add, Ident("foo").fix, Ident("baz").fix).fix.toJs must_==
      Js.Ternary(
        Js.BinOp("&&",
          Js.BinOp("!=", Js.Ident("foo"), Js.Null),
          Js.BinOp("!=", Js.Ident("baz"), Js.Null)),
        Js.BinOp("+", Js.Ident("foo"), Js.Ident("baz")),
        Js.Null)
    }

    "avoid repeating null checks in consequent" in {
      val expr = BinOp(Neq,
        Literal(Js.Num(-1.0,false)).fix,
        Call(Select(Select(Ident("this").fix, "loc").fix, "indexOf").fix,
          List(Select(Ident("this").fix, "pop").fix)).fix).fix
      val exp = Js.Ternary(
        Js.BinOp("!=",
          Js.Ternary(
            Js.BinOp("&&",
              Js.BinOp("!=", Js.Select(Js.This, "loc"), Js.Null),
              Js.BinOp("!=", Js.Select(Js.Select(Js.This, "loc"), "indexOf"), Js.Null)),
            Js.Call(
              Js.Select(Js.Select(Js.This, "loc"), "indexOf"),
              List(
                Js.Ternary(
                  Js.BinOp("!=",Js.This, Js.Null),
                  Js.Select(Js.This, "pop"),
                  Js.Undefined))),
            Js.Undefined),
          Js.Null),
        Js.BinOp("!==",
          Js.Num(-1, false),
          Js.Call(Js.Select(Js.Select(Js.This, "loc"), "indexOf"), List(Js.Select(Js.This, "pop")))),
        Js.Null)
      expr.toJs.render(0) must_== exp.render(0)
    }

    "de-sugar Let as AnonFunDecl" in {
      val let = Let(ListMap(
        "a" -> Literal(Js.Num(1, false)).fix),
        Ident("a").fix).fix

      let.toJs must_==
        Js.Call(
          Js.AnonFunDecl(
            List("a"),
            List(Js.Return(Js.Ident("a")))),
          List(Js.Num(1, false)))
    }

    "don't null-check method call on newly-constructed instance" in {
      val expr = Call(Select(New("Date", List[Term[JsCore]]()).fix, "getUTCSeconds").fix, List()).fix
      expr.toJs.render(0) must_== "(new Date()).getUTCSeconds()"
    }

    "don't null-check method call on newly-constructed Array" in {
      val expr = Call(Select(Arr(List(Literal(Js.Num(0, false)).fix, Literal(Js.Num(1, false)).fix)).fix, "indexOf").fix, List(Ident("x").fix)).fix
      expr.toJs.render(0) must_== "[0, 1].indexOf(x)"
    }

    "null-check method call on other value" in {
      val expr = Call(Select(Ident("value").fix, "getUTCSeconds").fix, List()).fix
      expr.toJs.render(0) must_== "((value != null) && (value.getUTCSeconds != null)) ? value.getUTCSeconds() : undefined"
    }

    "splice obj constructor" in {
      val expr = Splice(List(Obj(ListMap("foo" -> Select(Ident("bar").fix, "baz").fix)).fix)).fix
      expr.toJs.render(0) must_==
        "(function (__rez) { __rez.foo = (bar != null) ? bar.baz : undefined; return __rez })(\n  {  })"
    }

    "splice other expression" in {
      val expr = Splice(List(Ident("foo").fix)).fix
      expr.toJs.render(0) must_==
        """(function (__rez) {
          |  for (var __attr in (foo)) if (foo.hasOwnProperty(__attr)) __rez[__attr] = foo[__attr];
          |  return __rez
          |})(
          |  {  })""".stripMargin
    }
  }

  ">>>" should {
    "do _.foo, then _.bar" in {
      val x = JsCore.Ident("x").fix

      val a = JsMacro(JsCore.Select(_, "foo").fix)
      val b = JsMacro(JsCore.Select(_, "bar").fix)

      (a >>> b)(x).toJs.render(0) must_==
        "((x != null) && (x.foo != null)) ? x.foo.bar : undefined"
    }
  }

  "simplify" should {
    "inline select(obj)" in {
      val x = JsCore.Select(
        JsCore.Obj(ListMap(
          "a" -> JsCore.Ident("x").fix,
          "b" -> JsCore.Ident("y").fix
        )).fix,
        "a").fix

      x.simplify must_==
        JsCore.Ident("x").fix
    }
  }

  "JsMacro" should {
    "toString" should {
      "be simpler than the equivalent (safe) JS" in {
        val js = JsMacro(x => JsCore.Obj(ListMap(
          "a" -> JsCore.Select(x, "x").fix,
          "b" -> JsCore.Select(x, "y").fix)).fix)

        js.toString must beEqualTo("""{ "a": _.x, "b": _.y }""").ignoreSpace

        js(JsCore.Ident("_").fix).toJs.render(0) must beEqualTo(
          """{
               "a": (_ != null) ? _.x : undefined,
               "b": (_ != null) ? _.y : undefined
             }""").ignoreSpace
      }
    }
  }
}
