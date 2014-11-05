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
}