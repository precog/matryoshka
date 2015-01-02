package slamdata.engine.javascript

import org.specs2.mutable._

import slamdata.engine.{TreeMatchers}

import scala.collection.immutable.ListMap

class JsCoreSpecs extends Specification with TreeMatchers {
  import JsCore._
  
  "toJs" should {
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
        """(function (expr) {
          |    return (((typeof expr) !== "undefined") && (expr !== null)) ? expr.bar : {}.__undef;
          |  })((function (expr) {
          |    return (((typeof expr) !== "undefined") && (expr !== null)) ? expr.foo : {}.__undef;
          |  })(x))""".stripMargin
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
