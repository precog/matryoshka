package quasar

import quasar.Predef._
import RenderTree.ops._
import quasar.fp._
import quasar.recursionschemes._

import scala.reflect.ClassTag

import org.specs2.matcher._
import scalaz._, Scalaz._

trait TreeMatchers {
  def beTree[A: RenderTree](expected: A): Matcher[A] = new Matcher[A] {
    def apply[S <: A](s: Expectable[S]) = {
      val v = s.value
      val diff = (RenderTree[A].render(v) diff expected.render).shows
      result(v == expected, s"trees match:\n$diff", s"trees do not match:\n$diff", s)
    }
  }
}

trait TermLogicalPlanMatchers {
  case class equalToPlan(expected: Fix[LogicalPlan])
      extends Matcher[Fix[LogicalPlan]] {
    def apply[S <: Fix[LogicalPlan]](s: Expectable[S]) = {
      val normed = FunctorT[Cofree[?[_], Fix[LogicalPlan]]].transCata(attrSelf(s.value))(repeatedly[Cofree[?[_], Fix[LogicalPlan]], LogicalPlan](Optimizer.simplifyƒ[Cofree[?[_], Fix[LogicalPlan]]]))
      val diff = (Recursive[Cofree[?[_], Fix[LogicalPlan]]].forget(normed).render diff expected.render).shows
      result(
        expected ≟ Recursive[Cofree[?[_], Fix[LogicalPlan]]].forget(normed),
        "\ntrees are equal:\n" + diff,
        "\ntrees are not equal:\n" + diff +
          "\noriginal was:\n" + normed.head.render.shows,
        s)
    }
  }
}
