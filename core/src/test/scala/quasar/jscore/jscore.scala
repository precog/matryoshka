package quasar.jscore

import quasar.Predef._
import quasar.RenderTree
import quasar.javascript.Js
import quasar.fp._
import quasar.recursionschemes._
import quasar.TreeMatchers

import org.specs2.mutable._
import scalaz._, Scalaz._

class JsCoreSpecs extends Specification with TreeMatchers {
  "toJs" should {
    "handle projecting a value safely" in {
      Access(ident("foo"), ident("bar")).toJs must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.Access(Js.Ident("foo"), Js.Ident("bar")),
        Js.Undefined)
    }

    "prevent projecting from null" in {
      Access(Literal(Js.Null), ident("bar")).toJs must_==
      Js.Undefined
    }

    "not protect projections from literals" in {
      Access(Literal(Js.Num(1, false)), ident("bar")).toJs must_==
      Js.Access(Js.Num(1, false), Js.Ident("bar"))
    }

    "handle calling a projection safely" in {
      val expr = Call(Select(ident("foo"), "bar"), Nil)
      val exp =
        Js.Ternary(
          Js.BinOp("&&",
            Js.BinOp("!=", Js.Ident("foo"), Js.Null),
            Js.BinOp("!=", Js.Select(Js.Ident("foo"), "bar"), Js.Null)),
          Js.Call(Js.Select(Js.Ident("foo"), "bar"), Nil),
          Js.Undefined)

      expr.toJs.pprint(0) must_== exp.pprint(0)
    }

    "handle assigning to a property safely" in {
      safeAssign(Select(ident("foo"), "bar"), ident("baz")) must_==
      Js.Ternary(Js.BinOp("!=", Js.Ident("foo"), Js.Null),
        Js.BinOp("=", Js.Select(Js.Ident("foo"), "bar"), Js.Ident("baz")),
        Js.Undefined)
    }

    "handle binary operations safely" in {
      BinOp(Add, ident("foo"), ident("baz")).toJs must_==
      Js.Ternary(
        Js.BinOp("&&",
          Js.BinOp("!=", Js.Ident("foo"), Js.Null),
          Js.BinOp("!=", Js.Ident("baz"), Js.Null)),
        Js.BinOp("+", Js.Ident("foo"), Js.Ident("baz")),
        Js.Null)
    }

    "avoid repeating null checks in consequent" in {
      val expr = BinOp(Neq,
        Literal(Js.Num(-1.0,false)),
        Call(Select(Select(ident("this"), "loc"), "indexOf"),
          List(Select(ident("this"), "pop"))))
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
      expr.toJs.pprint(0) must_== exp.pprint(0)
    }

    "de-sugar Let as AnonFunDecl" in {
      val let = Let(
        Name("a"),
        Literal(Js.Num(1, false)),
        ident("a"))

      let.toJs must_==
        Js.Call(
          Js.AnonFunDecl(
            List("a"),
            List(Js.Return(Js.Ident("a")))),
          List(Js.Num(1, false)))
    }

    "de-sugar nested Lets as single AnonFunDecl" in {
      val let = Let(
        Name("a"),
        Literal(Js.Num(1, false)),
        Let(
          Name("b"),
          Literal(Js.Num(1, false)),
          BinOp(Add, ident("a"), ident("a"))))

      let.toJs must_==
        Js.Call(
          Js.AnonFunDecl(
            List("a", "b"),
            List(Js.Return(Js.BinOp("+", Js.Ident("a"), Js.Ident("b"))))),
          List(Js.Num(1, false)))
    }.pendingUntilFixed

    "don't null-check method call on newly-constructed instance" in {
      val expr = Call(Select(New(Name("Date"), List[JsCore]()), "getUTCSeconds"), List())
      expr.toJs.pprint(0) must_== "(new Date()).getUTCSeconds()"
    }

    "don't null-check method call on newly-constructed Array" in {
      val expr = Call(Select(Arr(List(Literal(Js.Num(0, false)), Literal(Js.Num(1, false)))), "indexOf"), List(ident("x")))
      expr.toJs.pprint(0) must_== "[0, 1].indexOf(x)"
    }

    "null-check method call on other value" in {
      val expr = Call(Select(ident("value"), "getUTCSeconds"), List())
      expr.toJs.pprint(0) must_== "((value != null) && (value.getUTCSeconds != null)) ? value.getUTCSeconds() : undefined"
    }

    "splice obj constructor" in {
      val expr = SpliceObjects(List(Obj(ListMap(Name("foo") -> Select(ident("bar"), "baz")))))
      expr.toJs.pprint(0) must_==
        "(function (__rez) { __rez.foo = (bar != null) ? bar.baz : undefined; return __rez })(\n  {  })"
    }

    "splice other expression" in {
      val expr = SpliceObjects(List(ident("foo")))
      expr.toJs.pprint(0) must_==
        """(function (__rez) {
          |  for (var __attr in (foo)) if (foo.hasOwnProperty(__attr)) __rez[__attr] = foo[__attr];
          |  return __rez
          |})(
          |  {  })""".stripMargin
    }

    "splice arrays" in {
      val expr = SpliceArrays(List(
        Arr(List(
          Select(ident("foo"), "bar"))),
        ident("foo")))
      expr.toJs.pprint(0) must_==
      """(function (__rez) {
        |  __rez.push((foo != null) ? foo.bar : undefined);
        |  for (var __elem in (foo)) if (foo.hasOwnProperty(__elem)) __rez.push(foo[__elem]);
        |  return __rez
        |})(
        |  [])""".stripMargin
    }
  }

  "simplify" should {
    "inline select(obj)" in {
      val x = Select(
        Obj(ListMap(
          Name("a") -> ident("x"),
          Name("b") -> ident("y")
        )),
        "a")

      x.simplify must_== ident("x")
    }
  }

  "RenderTree" should {
    "render flat expression" in {
      val expr = Select(ident("foo"), "bar")
      expr.shows must_== "JsCore(foo.bar)"
    }

    "render obj as nested" in {
      val expr = Obj(ListMap(Name("foo") -> ident("bar")))
      expr.shows must_==
        """Obj
          |╰─ Key(foo: bar)""".stripMargin
    }

    "render mixed" in {
      val expr = Obj(ListMap(Name("foo") -> Call(Select(ident("bar"), "baz"), List())))
      expr.shows must_==
        """Obj
          |╰─ Key(foo: bar.baz())""".stripMargin
    }
  }

  "JsFn" should {
    "substitute with shadowing Let" in {
      val fn = JsFn(Name("x"),
        BinOp(Add,
          ident("x"),
          Let(Name("x"),
            BinOp(Add,
              Literal(Js.Num(1, false)),
              ident("x")),
            ident("x"))))
      val exp = BinOp(Add,
        Literal(Js.Num(2, false)),
        Let(Name("x"),
          BinOp(Add,
            Literal(Js.Num(1, false)),
            Literal(Js.Num(2, false))),
          ident("x")))

      fn(Literal(Js.Num(2, false))) must_== exp
    }

    "substitute with shadowing Fun" in {
      val fn = JsFn(Name("x"),
        BinOp(Add,
          ident("x"),
          Call(
            Fun(List(Name("x")),
              ident("x")),
            List(Literal(Js.Num(1, false))))))
      val exp = BinOp(Add,
        Literal(Js.Num(2, false)),
        Call(
          Fun(List(Name("x")),
            ident("x")),
          List(Literal(Js.Num(1, false)))))

      fn(Literal(Js.Num(2, false))) must_== exp
    }

    "toString" should {
      "be simpler than the equivalent (safe) JS" in {
        val js = JsFn(Name("val"), Obj(ListMap(
          Name("a") -> Select(ident("val"), "x"),
          Name("b") -> Select(ident("val"), "y"))))

        js.toString must beEqualTo("""{ "a": _.x, "b": _.y }""").ignoreSpace

        js(ident("_")).toJs.pprint(0) must beEqualTo(
          """{
               "a": (_ != null) ? _.x : undefined,
               "b": (_ != null) ? _.y : undefined
             }""").ignoreSpace
      }
    }

    ">>>" should {
      "do _.foo, then _.bar" in {
        val x = ident("x")

        val a = JsFn(Name("val"), Select(ident("val"), "foo"))
        val b = JsFn(Name("val"), Select(ident("val"), "bar"))

        (a >>> b)(x).toJs.pprint(0) must_==
          "((x != null) && (x.foo != null)) ? x.foo.bar : undefined"
      }
    }
  }
}
