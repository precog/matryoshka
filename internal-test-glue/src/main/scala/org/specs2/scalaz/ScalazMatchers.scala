package org.specs2.scalaz

import scalaz._

import org.specs2.matcher._

trait ScalazMatchers extends ValidationMatchers with DisjunctionMatchers { outer =>

  /** Equality matcher with a [[scalaz.Equal]] typeclass */
  def equal[T : Equal : Show](expected: T): Matcher[T] = new Matcher[T] {
    def apply[S <: T](actual: Expectable[S]): MatchResult[S] = {
      val actualT = actual.value.asInstanceOf[T]
      def test = Equal[T].equal(expected, actualT)
      def koMessage = "%s !== %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      def okMessage = "%s === %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      Matcher.result(test, okMessage, koMessage, actual)
    }
  }

  class ScalazBeHaveMatchers[T : Equal : Show](result: MatchResult[T]) {
    def equal(t: T) = result(outer.equal[T](t)(Equal[T], Show[T]))
  }

  import scala.language.implicitConversions
  implicit def scalazBeHaveMatcher[T : Equal : Show](result: MatchResult[T]) = new ScalazBeHaveMatchers(result)
}

object ScalazMatchers extends ScalazMatchers

// vim: expandtab:ts=2:sw=2
