package org.specs2.scalaz

import scalaz.{ -\/, \/, \/- }

import org.specs2.matcher._
import org.specs2.text.Quote._
import org.specs2.execute.{ Failure, Result }

trait DisjunctionMatchers { outer =>

  def beRightDisjunction[T](t: => T) =
    new Matcher[\/[_, T]] {
      def apply[S <: \/[_, T]](value: Expectable[S]) = {
        val expected = t
        result(
          value.value == \/.right(t),
          value.description + " is \\/- with value" + q(expected),
          value.description + " is not \\/- with value" + q(expected),
          value
        )
      }
    }

  def beRightDisjunction[T] = new RightDisjunctionMatcher[T]

  class RightDisjunctionMatcher[T] extends Matcher[\/[_, T]] {
    def apply[S <: \/[_, T]](value: Expectable[S]) = {
      result(
        value.value.isRight,
        value.description + " is \\/-",
        value.description + " is not \\/-",
        value
      )
    }

    def like(f: PartialFunction[T, MatchResult[_]]) = this and partialMatcher(f)

    private def partialMatcher(f: PartialFunction[T, MatchResult[_]]) = new Matcher[\/[_, T]] {
      def apply[S <: \/[_, T]](value: Expectable[S]) = {
        val res: Result = value.value match {
          case \/-(t) if f.isDefinedAt(t)  => f(t).toResult
          case \/-(t) if !f.isDefinedAt(t) => Failure("function undefined")
          case other                            => Failure("no match")
        }
        result(
          res.isSuccess,
          value.description + " is \\/-[T] and " + res.message,
          value.description + " is \\/-[T] but " + res.message,
          value
        )
      }
    }
  }

  def rightDisjunction[T](t: => T) = beRightDisjunction(t)
  def be_\/-[T](t: => T) = beRightDisjunction(t)
  def rightDisjunction[T] = beRightDisjunction
  def be_\/-[T]: Matcher[\/[_, T]] = beRightDisjunction

  def beLeftDisjunction[T](t: => T) = new Matcher[\/[T, _]] {
    def apply[S <: \/[T, _]](value: Expectable[S]) = {
      val expected = t
      result(
        value.value == \/.left(t),
        value.description + " is -\\/ with value" + q(expected),
        value.description + " is not -\\/ with value" + q(expected),
        value
      )
    }
  }

  def beLeftDisjunction[T] = new LeftDisjunctionMatcher[T]
  class LeftDisjunctionMatcher[T] extends Matcher[\/[T, _]] {
    def apply[S <: \/[T, _]](value: Expectable[S]) = {
      result(
        value.value.isLeft,
        value.description + " is -\\/",
        value.description + " is not -\\/",
        value
      )
    }

    def like(f: PartialFunction[T, MatchResult[_]]) = this and partialMatcher(f)

    private def partialMatcher(f: PartialFunction[T, MatchResult[_]]) = new Matcher[\/[T, _]] {
      def apply[S <: \/[T, _]](value: Expectable[S]) = {
        val res: Result = value.value match {
          case -\/(t) if f.isDefinedAt(t)  => f(t).toResult
          case -\/(t) if !f.isDefinedAt(t) => Failure("function undefined")
          case other                            => Failure("no match")
        }
        result(
          res.isSuccess,
          value.description + " is -\\/[T] and " + res.message,
          value.description + " is -\\/[T] but " + res.message,
          value
        )
      }
    }
  }

  def leftDisjunction[T](t: => T) = beLeftDisjunction(t)
  def be_-\/[T](t: => T) = beLeftDisjunction(t)
  def leftDisjunction[T] = beLeftDisjunction
  def be_-\/[T]: Matcher[\/[T, _]] = beLeftDisjunction

  implicit def toDisjunctionResultMatcher[F, S](result: MatchResult[\/[F, S]]) =
    new DisjunctionResultMatcher(result)

  class DisjunctionResultMatcher[F, S](result: MatchResult[\/[F, S]]) {
    def leftDisjunction(f: => F) = result(outer beLeftDisjunction f)
    def beLeftDisjunction(f: => F) = result(outer beLeftDisjunction f)
    def be_-\/(f: => F) = result(outer beLeftDisjunction f)
    def rightDisjunction(s: => S) = result(outer beRightDisjunction s)
    def beRightDisjunction(s: => S) = result(outer beRightDisjunction s)
    def be_\/-(s: => S) = result(outer beRightDisjunction s)

    def leftDisjunction = result(outer.beLeftDisjunction)
    def beLeftDisjunction = result(outer.beLeftDisjunction)
    def be_-\/ = result(outer.beLeftDisjunction)
    def rightDisjunction = result(outer.beRightDisjunction)
    def beRightDisjunction = result(outer.beRightDisjunction)
    def be_\/- = result(outer.beRightDisjunction)
  }
}

object DisjunctionMatchers extends DisjunctionMatchers

// vim: expandtab:ts=2:sw=2
