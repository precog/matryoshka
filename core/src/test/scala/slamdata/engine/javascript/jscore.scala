package slamdata.engine.javascript

import org.specs2.mutable._

import slamdata.engine.{TreeMatchers}

import scala.collection.immutable.ListMap

class JsCoreSpecs extends Specification with TreeMatchers {
  import JsCore._
  
  "toJs" should {
    "handle projecting a value safely" in {
      Access(Ident("foo").fix, Ident("bar").fix).fix.toJs must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.Access(Js.Ident("foo"), Js.Ident("bar")),
        Js.Ident("undefined"))
    }

    "prevent projecting from null" in {
      Access(Literal(Js.Null).fix, Ident("bar").fix).fix.toJs must_==
      Js.Ident("undefined")
    }

    "not protect projections from literals" in {
      Access(Literal(Js.Num(1, false)).fix, Ident("bar").fix).fix.toJs must_==
      Js.Access(Js.Num(1, false), Js.Ident("bar"))
    }

    "handle calling a projection safely" in {
      Call(Select(Ident("foo").fix, "bar").fix, Nil).fix.toJs must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.Call(Js.Select(Js.Ident("foo"), "bar"), Nil),
        Js.Null)
    }

    "handle assigning to a property safely" in {
      BinOp("=", Select(Ident("foo").fix, "bar").fix, Ident("baz").fix).fix.toJs must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.BinOp("=", Js.Select(Js.Ident("foo"), "bar"), Js.Ident("baz")),
        Js.Null)
    }

    "handle binary operations safely" in {
      BinOp("+", Ident("foo").fix, Ident("baz").fix).fix.toJs must_==
      Js.Ternary(
        Js.BinOp("&&",
          Js.BinOp("!=", Js.Ident("foo"), Js.Null),
          Js.BinOp("!=", Js.Ident("baz"), Js.Null)),
        Js.BinOp("+", Js.Ident("foo"), Js.Ident("baz")),
        Js.Null)
    }

    "avoid repeating null checks in consequent" in {
      BinOp("!==",
        Literal(Js.Num(-1.0,false)).fix,
        Call(Select(Select(Ident("this").fix, "loc").fix, "indexOf").fix,
          List(Select(Ident("this").fix, "pop").fix)).fix).fix.toJs must_==
      Js.Ternary(
        Js.BinOp("!=",
          Js.Ternary(
            Js.BinOp("!=",
              Js.Ternary(
                Js.BinOp("!=", Js.This, Js.Null),
                Js.Select(Js.This, "loc"),
                Js.Ident("undefined")),
              Js.Null),
            Js.Call(Js.Select(Js.Select(Js.This, "loc"), "indexOf"), List(
              Js.Ternary(
                Js.BinOp("!=", Js.This, Js.Null),
                Js.Select(Js.This, "pop"),
                Js.Ident("undefined")))),
            Js.Null),
          Js.Null),
        Js.BinOp("!==",
          Js.Num(-1.0,false),
          Js.Call(Js.Select(Js.Select(Js.This, "loc"), "indexOf"), List(
            Js.Select(Js.This, "pop")))),
        Js.Null)
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
  }

  ">>>" should {
    "do _.foo, then _.bar" in {
      val x = JsCore.Ident("x").fix

      val a = JsMacro(JsCore.Select(_, "foo").fix)
      val b = JsMacro(JsCore.Select(_, "bar").fix)
      
      (a >>> b)(x).toJs.render(0) must_==
        "(((x != null) ? x.foo : undefined) != null) ? x.foo.bar : undefined"
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
}
