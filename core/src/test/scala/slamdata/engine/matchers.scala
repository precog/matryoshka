package slamdata.engine

import slamdata.Predef._

import scala.reflect.ClassTag

import org.specs2.matcher._

import scalaz._

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
  import slamdata.engine.analysis.fixplate._
  import slamdata.engine.fp._
  import slamdata.engine.RenderTree

  case class equalToPlan(expected: Term[LogicalPlan]) extends Matcher[Term[LogicalPlan]] {
    val equal = Equal[Term[LogicalPlan]].equal _

    def apply[S <: Term[LogicalPlan]](s: Expectable[S]) = {
      def diff(l: S, r: Term[LogicalPlan]): String = {
        val lt = RenderTree[Term[LogicalPlan]].render(l)
        val rt = RenderTree[Term[LogicalPlan]].render(r)
        RenderTree.show(lt diff rt)(new RenderTree[RenderedTree] { override def render(v: RenderedTree) = v }).toString
      }
      result(equal(expected, s.value),
             "\ntrees are equal:\n" + diff(s.value, expected),
             "\ntrees are not equal:\n" + diff(s.value, expected),
             s)
    }
  }
}
