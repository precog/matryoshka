package org.specs2.scalaz

import scalaz.{ Failure => ZFailure, Success => ZSuccess, Validation }

import org.specs2.matcher._
import org.specs2.text.Quote._
import org.specs2.execute.{ Failure, Result }

trait ValidationMatchers { outer =>

  def beSuccessful[T](t: => T) =
    new Matcher[Validation[_, T]] {
      def apply[S <: Validation[_, T]](value: Expectable[S]) = {
        val expected = t
        result(
          value.value == ZSuccess(t),
          value.description + " is Success with value" + q(expected),
          value.description + " is not Success with value" + q(expected),
          value
        )
      }
    }

  def beSuccessful[T] = new SuccessValidationMatcher[T]

  class SuccessValidationMatcher[T] extends Matcher[Validation[_, T]] {
    def apply[S <: Validation[_, T]](value: Expectable[S]) = {
      result(
        value.value.isSuccess,
        value.description + " is Success",
        value.description + " is not Success",
        value
      )
    }

    def like(f: PartialFunction[T, MatchResult[_]]) = this and partialMatcher(f)

    private def partialMatcher(f: PartialFunction[T, MatchResult[_]]) = new Matcher[Validation[_, T]] {
      def apply[S <: Validation[_, T]](value: Expectable[S]) = {
        val res: Result = value.value match {
          case ZSuccess(t) if f.isDefinedAt(t)  => f(t).toResult
          case ZSuccess(t) if !f.isDefinedAt(t) => Failure("function undefined")
          case other                            => Failure("no match")
        }
        result(
          res.isSuccess,
          value.description + " is Success[T] and " + res.message,
          value.description + " is Success[T] but " + res.message,
          value
        )
      }
    }
  }

  def successful[T](t: => T) = beSuccessful(t)
  def successful[T] = beSuccessful

  def beFailing[T](t: => T) = new Matcher[Validation[T, _]] {
    def apply[S <: Validation[T, _]](value: Expectable[S]) = {
      val expected = t
      result(
        value.value == ZFailure(t),
        value.description + " is Failure with value" + q(expected),
        value.description + " is not Failure with value" + q(expected),
        value
      )
    }
  }

  def beFailing[T] = new FailureMatcher[T]
  class FailureMatcher[T] extends Matcher[Validation[T, _]] {
    def apply[S <: Validation[T, _]](value: Expectable[S]) = {
      result(
        value.value.isFailure,
        value.description + " is Failure",
        value.description + " is not Failure",
        value
      )
    }

    def like(f: PartialFunction[T, MatchResult[_]]) = this and partialMatcher(f)

    private def partialMatcher(f: PartialFunction[T, MatchResult[_]]) = new Matcher[Validation[T, _]] {
      def apply[S <: Validation[T, _]](value: Expectable[S]) = {
        val res: Result = value.value match {
          case ZFailure(t) if f.isDefinedAt(t)  => f(t).toResult
          case ZFailure(t) if !f.isDefinedAt(t) => Failure("function undefined")
          case other                            => Failure("no match")
        }
        result(
          res.isSuccess,
          value.description + " is Failure[T] and " + res.message,
          value.description + " is Failure[T] but " + res.message,
          value
        )
      }
    }
  }

  def failing[T](t: => T) = beFailing(t)
  def failing[T] = beFailing

  import scala.language.implicitConversions
  implicit def toValidationResultMatcher[F, S](result: MatchResult[Validation[F, S]]) =
    new ValidationResultMatcher(result)

  class ValidationResultMatcher[F, S](result: MatchResult[Validation[F, S]]) {
    def failing(f: => F) = result(outer beFailing f)
    def beFailing(f: => F) = result(outer beFailing f)
    def successful(s: => S) = result(outer beSuccessful s)
    def beSuccessful(s: => S) = result(outer beSuccessful s)

    def failing = result(outer.beFailing)
    def beFailing = result(outer.beFailing)
    def successful = result(outer.beSuccessful)
    def beSuccessful = result(outer.beSuccessful)
  }
}

object ValidationMatchers extends ValidationMatchers

// vim: expandtab:ts=2:sw=2
