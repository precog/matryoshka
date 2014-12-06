package slamdata.engine

import org.specs2.mutable._

import scalaz._

import argonaut._ 
import Argonaut._

class RenderedTreeSpec extends Specification {
  "RenderedTree.diff" should {

    "find no differences" in {
      val t = NonTerminal("A", 
              Terminal("B") :: Terminal("C") :: Nil)
      t.diff(t) must_== t
    }

    "find simple difference" in {
      val t1 = Terminal("A")
      val t2 = Terminal("B")
      t1.diff(t2) must_==
        NonTerminal("", 
          Terminal("A", List(">>>")) ::
            Terminal("B", List("<<<")) ::
            Nil,
          "[Root differs]" :: Nil)
    }

    "find simple difference in parent" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("C", Terminal("B") :: Nil)
      t1.diff(t2) must_== 
        NonTerminal("", 
          NonTerminal("A", Terminal("B") :: Nil, List(">>>")) ::
            NonTerminal("C", Terminal("B") :: Nil, List("<<<")) ::
            Nil,
          "[Root differs]" :: Nil)
    }

    "find added child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("C", "<<<" :: Nil) :: Nil)
    }

    "find deleted child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("C", ">>>" :: Nil) :: Nil)
    }

    "find simple difference in child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Nil)
      t1.diff(t2) must_==
        NonTerminal("A", 
          Terminal("B", ">>>" :: Nil) ::
            Terminal("C", "<<<" :: Nil) ::
            Nil)
    }

    "find multiple changed children" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Terminal("D") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Terminal("C1") :: Terminal("D") :: Nil)
      t1.diff(t2) must_==
        NonTerminal("A", 
          Terminal("B", ">>>" :: Nil) :: 
            Terminal("C") :: 
            Terminal("C1", "<<<" :: Nil) :: 
            Terminal("D") :: Nil)
    }

    "find added grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("C", "<<<" :: Nil) :: Nil) :: Nil)
    }

    "find deleted grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("C", ">>>" :: Nil) :: Nil) :: Nil)
    }

    "find different nodeType at root" in {
      val t1 = Terminal("A", List("green"))
      val t2 = Terminal("A", List("blue"))
      t1.diff(t2) must_== NonTerminal("",
                            Terminal("A", List(">>> green")) ::
                              Terminal("A", List("<<< blue")) :: 
                              Nil,
                            List("[Root differs]"))
    }

    "find different nodeType" in {
      val t1 = NonTerminal("", Terminal("A", List("green")) :: Nil, List("root"))
      val t2 = NonTerminal("", Terminal("A", List("blue")) :: Nil, List("root"))
      t1.diff(t2) must_== NonTerminal("",
                            Terminal("A", List(">>> green")) ::
                              Terminal("A", List("<<< blue")) :: 
                              Nil,
                            List("root"))
    }

  }
  
  "RenderedTreeEncodeJson" should {

    "encode Terminal" in {
      Terminal("A").asJson must_== Json("label" := "A")
    }

    "encode Terminal with type" in {
      Terminal("A", List("green")).asJson must_== Json("type" := "green", "label" := "A")
    }

    "encode NonTerminal with no children" in {
      NonTerminal("A", Nil).asJson must_== Json("label" := "A")
    }

    "encode NonTerminal with type and no children" in {
      NonTerminal("A", Nil, List("green")).asJson must_== Json("type" := "green", "label" := "A")
    }

    "encode NonTerminal with one child" in {
      NonTerminal("A", Terminal("B") :: Nil).asJson must_== 
        Json(
          "label" := "A", 
          "children" := Json("label" := "B") :: Nil)
    }

    "encode NonTerminal with one child and type" in {
      NonTerminal("A", Terminal("B") :: Nil, List("green")).asJson must_== 
        Json(
          "type" := "green",
          "label" := "A", 
          "children" := Json("label" := "B") :: Nil)
    }

  }
}