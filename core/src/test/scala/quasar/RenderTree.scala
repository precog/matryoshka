package quasar

import quasar.Predef._

import argonaut._, Argonaut._
import org.specs2.mutable._

class RenderedTreeSpec extends Specification {
  "RenderedTree.diff" should {

    "find no differences" in {
      val t = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal(Nil, Some("C")) :: Nil)
      t.diff(t) must_== t
    }

    "find simple difference" in {
      val t1 = Terminal(Nil, Some("A"))
      val t2 = Terminal(Nil, Some("B"))
      t1.diff(t2) must_==
        NonTerminal("[Root differs]" :: Nil, None,
          Terminal(List(">>>"), Some("A")) ::
            Terminal(List("<<<"), Some("B")) ::
            Nil)
    }

    "find simple difference in parent" in {
      val t1 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil)
      val t2 = NonTerminal(Nil, Some("C"), Terminal(Nil, Some("B")) :: Nil)
      t1.diff(t2) must_==
        NonTerminal("[Root differs]" :: Nil, None,
          NonTerminal(List(">>>"), Some("A"), Terminal(Nil, Some("B")) :: Nil) ::
            NonTerminal(List("<<<"), Some("C"), Terminal(Nil, Some("B")) :: Nil) ::
            Nil)
    }

    "find added child" in {
      val t1 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal(Nil, Some("C")) :: Nil)
      t1.diff(t2) must_== NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal("<<<" :: Nil, Some("C")) :: Nil)
    }

    "find deleted child" in {
      val t1 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal(Nil, Some("C")) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil)
      t1.diff(t2) must_== NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal(">>>" :: Nil, Some("C")) :: Nil)
    }

    "find simple difference in child" in {
      val t1 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("C")) :: Nil)
      t1.diff(t2) must_==
        NonTerminal(Nil, Some("A"),
          Terminal(">>>" :: Nil, Some("B")) ::
            Terminal("<<<" :: Nil, Some("C")) ::
            Nil)
    }

    "find multiple changed children" in {
      val t1 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Terminal(Nil, Some("C")) :: Terminal(Nil, Some("D")) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), Terminal(Nil, Some("C")) :: Terminal(Nil, Some("C1")) :: Terminal(Nil, Some("D")) :: Nil)
      t1.diff(t2) must_==
        NonTerminal(Nil, Some("A"),
          Terminal(">>>" :: Nil, Some("B")) ::
            Terminal(Nil, Some("C")) ::
            Terminal("<<<" :: Nil, Some("C1")) ::
            Terminal(Nil, Some("D")) :: Nil)
    }

    "find added grand-child" in {
      val t1 = NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Nil) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Terminal(Nil, Some("C")) :: Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Terminal("<<<" :: Nil, Some("C")) :: Nil) :: Nil)
    }

    "find deleted grand-child" in {
      val t1 = NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Terminal(Nil, Some("C")) :: Nil) :: Nil)
      val t2 = NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal(Nil, Some("A"), NonTerminal(Nil, Some("B"), Terminal(">>>" :: Nil, Some("C")) :: Nil) :: Nil)
    }

    "find different nodeType at root" in {
      val t1 = Terminal(List("green"), Some("A"))
      val t2 = Terminal(List("blue"), Some("A"))
      t1.diff(t2) must_== NonTerminal(List("[Root differs]"), None,
                            Terminal(List(">>> green"), Some("A")) ::
                              Terminal(List("<<< blue"), Some("A")) ::
                              Nil)
    }

    "find different nodeType" in {
      val t1 = NonTerminal(List("root"), None, Terminal(List("green"), Some("A")) :: Nil)
      val t2 = NonTerminal(List("root"), None, Terminal(List("blue"), Some("A")) :: Nil)
      t1.diff(t2) must_== NonTerminal(List("root"), None,
                            Terminal(List(">>> green"), Some("A")) ::
                              Terminal(List("<<< blue"), Some("A")) ::
                              Nil)
    }

    "find different nodeType (compound type; no labels)" in {
      val t1 = NonTerminal(List("root"), None, Terminal(List("red", "color"), None) :: Terminal(List("green", "color"), None) :: Nil)
      val t2 = NonTerminal(List("root"), None, Terminal(List("red", "color"), None) :: Terminal(List("blue", "color"), None) :: Nil)
      t1.diff(t2) must_== NonTerminal(List("root"), None,
                            Terminal(List("red", "color"), None) ::
                            Terminal(List(">>> green", "color"), None) ::
                              Terminal(List("<<< blue", "color"), None) ::
                              Nil)
    }
  }

  "RenderedTreeEncodeJson" should {

    "encode Terminal" in {
      Terminal(Nil, Some("A")).asJson must_== Json("label" := "A")
    }

    "encode Terminal with type" in {
      Terminal("green" :: Nil, Some("A")).asJson must_== Json("type" := "green", "label" := "A")
    }

    "encode Terminal with complex type" in {
      Terminal("inner" :: "outer" :: Nil, Some("A")).asJson must_== Json("type" := "outer/inner", "label" := "A")
    }

    "encode NonTerminal with no children" in {
      NonTerminal(Nil, Some("A"), Nil).asJson must_== Json("label" := "A")
    }

    "encode NonTerminal with type and no children" in {
      NonTerminal("green" :: Nil, Some("A"), Nil).asJson must_== Json("type" := "green", "label" := "A")
    }

    "encode NonTerminal with one child" in {
      NonTerminal(Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil).asJson must_==
        Json(
          "label" := "A",
          "children" := Json("label" := "B") :: Nil)
    }

    "encode NonTerminal with one child and type" in {
      NonTerminal("green" :: Nil, Some("A"), Terminal(Nil, Some("B")) :: Nil).asJson must_==
        Json(
          "type" := "green",
          "label" := "A",
          "children" := Json("label" := "B") :: Nil)
    }
  }
}
