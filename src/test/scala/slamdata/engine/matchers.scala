// After etorreborre: https://groups.google.com/forum/#!topic/specs2-users/V1kQmiV0Soo
// Also https://groups.google.com/forum/#!topic/specs2-users/Woq6KHCK4xo

package org.specs2.matcher  // required to import the implicits(?)

import scala.reflect.ClassTag

import org.specs2.mutable._
//import org.specs2.matcher.{Matcher, MatchersImplicits}
import MatchersImplicits._

import scalaz.{Validation, ValidationNel, NonEmptyList, Success, Failure}

trait ValidationMatchers {
  def beSuccess[E, A]: Matcher[Validation[E, A]] = 
    (v: Validation[E, A]) => (v.fold(_ => false, _ => true), s"$v is success", s"$v is not success")

  def beFailure[E, A]: Matcher[Validation[E, A]] = 
    (v: Validation[E, A]) => (v.fold(_ => true, _ => false), s"$v is failure", s"$v is not failure")

  def beSuccess[E, A](a: => A) = validationWith[E, A](Success(a))

  def beFailure[E, A](e: => E) = validationWith[E, A](Failure(e))

//  def successWithClass[E:ClassTag]:Matcher[Validation[Any,Any]] = (v:Validation[Any,Any]) => {
//    val requiredType = implicitly[ClassTag[E]].runtimeClass.toString
//    val result = v must beLike { case Success(e) => e must haveClass[E]}
//    (result.isSuccess,"v is a success of the required type" + requiredType, "v is not a success of " + requiredType )
//  }

  def beFailureWithClass[E:ClassTag]:Matcher[ValidationNel[Any,Any]] = (v:ValidationNel[Any,Any]) => {
    val requiredType = implicitly[ClassTag[E]].runtimeClass
    val result = v match { case Failure(NonEmptyList(e)) => e.getClass == requiredType }
    (result, "v is a failure of " + requiredType, "v is not a failure of " + requiredType)
  }

  private def validationWith[E, A](f: => Validation[E, A]): Matcher[Validation[E, A]] = (v: Validation[E, A]) => {
    val expected = f
    (expected == v, s"$v is $expected", s"$v is not $expected")
  }
}

object ValidationMatchers extends ValidationMatchers
