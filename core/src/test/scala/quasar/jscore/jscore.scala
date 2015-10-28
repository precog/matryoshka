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
    "de-sugar Let as AnonFunDecl" in {
      val let =
        Let(Name("a"), BinOp(Add, ident("c"), ident("d")),
          BinOp(Mult, ident("a"), ident("a")))

      let.toJs must_==
        Js.Call(
          Js.AnonFunDecl(List("a"),
            List(Js.Return(Js.BinOp("*", Js.Ident("a"), Js.Ident("a"))))),
          List(Js.BinOp("+", Js.Ident("c"), Js.Ident("d"))))
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

    "splice obj constructor" in {
      val expr = SpliceObjects(List(Obj(ListMap(Name("foo") -> Select(ident("bar"), "baz")))))
      expr.toJs.pprint(0) must_==
        "(function (__rez) { __rez.foo = bar.baz; return __rez })({  })"
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
        |  __rez.push(foo.bar);
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

    "inline object components" in {
      val x =
        Let(Name("a"),
          Obj(ListMap(
            Name("x") -> ident("y"),
            Name("q") -> If(ident("r"), Select(ident("r"), "foo"), ident("bar")))),
          Arr(List(
            Select(ident("a"), "x"),
            Select(ident("a"), "x"),
            Select(ident("a"), "q"))))

      x.simplify must_== Arr(List(ident("y"), ident("y"), If(ident("r"), Select(ident("r"), "foo"), ident("bar"))))
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
      "be the same as the equivalent JS" in {
        val js = JsFn(Name("val"), Obj(ListMap(
          Name("a") -> Select(ident("val"), "x"),
          Name("b") -> Select(ident("val"), "y"))))

        js.toString must beEqualTo("""{ "a": _.x, "b": _.y }""").ignoreSpace

        js(ident("_")).toJs.pprint(0) must beEqualTo(
          """{ "a": _.x, "b": _.y }""").ignoreSpace
      }
    }

    ">>>" should {
      "do _.foo, then _.bar" in {
        val x = ident("x")

        val a = JsFn(Name("val"), Select(ident("val"), "foo"))
        val b = JsFn(Name("val"), Select(ident("val"), "bar"))

        (a >>> b)(x).toJs.pprint(0) must_== "x.foo.bar"
      }
    }
  }
}
