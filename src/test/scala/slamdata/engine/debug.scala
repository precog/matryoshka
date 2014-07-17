package slamdata.engine

import scalaz._

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
      t1.diff(t2) must_== Terminal("[Changed] A -> B")
    }

    "find simple difference in parent" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("C", Terminal("B") :: Nil)
      t1.diff(t2) must_== NonTerminal("[Changed] A -> C", Terminal("B") :: Nil)
    }

    "find added child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("[Added] C") :: Nil)
    }

    "find deleted child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Nil)
      val t2 = NonTerminal("A", Terminal("B") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("B") :: Terminal("[Deleted] C") :: Nil)
    }

    "find simple difference in child" in {
      val t1 = NonTerminal("A", Terminal("B") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("[Changed] B -> C") :: Nil)
    }

    "find multiple changed children" in {
      val t1 = NonTerminal("A", Terminal("B") :: Terminal("C") :: Terminal("D") :: Nil)
      val t2 = NonTerminal("A", Terminal("C") :: Terminal("C1") :: Terminal("D") :: Nil)
      t1.diff(t2) must_== NonTerminal("A", Terminal("[Deleted] B") :: Terminal("C") :: Terminal("[Added] C1") :: Terminal("D"):: Nil)
    }

    "find added grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("[Added] C") :: Nil) :: Nil)
    }

    "find deleted grand-child" in {
      val t1 = NonTerminal("A", NonTerminal("B", Terminal("C") :: Nil) :: Nil)
      val t2 = NonTerminal("A", NonTerminal("B", Nil) :: Nil)
      t1.diff(t2) must_== NonTerminal("A", NonTerminal("B", Terminal("[Deleted] C") :: Nil) :: Nil)
    }

  }
}