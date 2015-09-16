package quasar

import quasar.Predef._
import RenderTree.ops._

import scala.reflect.ClassTag

import org.specs2.matcher._
import scalaz._, Scalaz._

trait TreeMatchers {
  def beTree[A](expected: A)(implicit RA: RenderTree[A]): Matcher[A] = new Matcher[A] {
    def apply[S <: A](s: Expectable[S]) = {
      val v = s.value
      def diff = (RA.render(v) diff RA.render(expected)).draw.mkString("\n")
      result(v == expected, s"trees match:\n$diff", s"trees do not match:\n$diff", s)
    }
  }
}

trait TermLogicalPlanMatchers {
  import quasar.fp._
  import quasar.recursionschemes._

  case class equalToPlan(expected: Fix[LogicalPlan]) extends Matcher[Fix[LogicalPlan]] {
    val equal = Equal[Fix[LogicalPlan]].equal _

    def apply[S <: Fix[LogicalPlan]](s: Expectable[S]) = {
      def diff(l: S, r: Fix[LogicalPlan]): String = {
        val lt = RenderTree[Fix[LogicalPlan]].render(l)
        (lt diff r.render).shows
      }
      result(equal(expected, s.value),
             "\ntrees are equal:\n" + diff(s.value, expected),
             "\ntrees are not equal:\n" + diff(s.value, expected),
             s)
    }
  }
}
