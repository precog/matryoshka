package slamdata.engine

import scalaz._

import argonaut._ 
import Argonaut._

import org.specs2.mutable._
import org.specs2.execute.PendingUntilFixed

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
      t1.diff(t2) must_== Terminal("A -> B", "[Changed]" :: Nil)
    }

    "find simple difference in parent" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("C", Terminal("B") :: Nil)
      t1.diff(t2) must_== NonTerminal("A -> C", Terminal("B") :: Nil, "[Changed]" :: Nil)
    }

    "find added child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("C", "[Added]" :: Nil) :: Nil)
    }

    "find deleted child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("C", "[Deleted]" :: Nil) :: Nil)
    }

    "find simple difference in child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B -> C", "[Changed]" :: Nil) :: Nil)
    }

    "find multiple changed children" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Terminal("D") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Terminal("C1") :: Terminal("D") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B", "[Deleted]" :: Nil) :: Terminal("C") :: Terminal("C1", "[Added]" :: Nil) :: Terminal("D"):: Nil)
    }

    "find added grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("C", "[Added]" :: Nil) :: Nil) :: Nil)
    }

    "find deleted grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("C", "[Deleted]" :: Nil) :: Nil) :: Nil)
    }

    "ignore simple difference in nodeType (and drop the type)" in {
      val t1 = Terminal("A", List("green"))
      val t2 = Terminal("A", List("blue"))
      t1.diff(t2) must_== Terminal("A")
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