package quasar.specs2

import quasar.Predef._
import quasar.{RenderTree, RenderedTree}, RenderTree.ops._
import quasar.fp._

import scala.reflect.ClassTag

import org.specs2.matcher._
import scalaz._, Scalaz._

trait ValidationMatchers {
  def beSuccessful[E, A] = new Matcher[Validation[E, A]] {
    def apply[S <: Validation[E, A]](s: Expectable[S]) = {
      val v = s.value

      result(
        v.fold(κ(false), κ(true)),
        s"$v is success",
        s"$v is not success",
        s
      )
    }
  }

  def beEqualIfSuccess[E, A](expected: Validation[E, A]) =
    new Matcher[Validation[E, A]] {
      def apply[S <: Validation[E, A]](s: Expectable[S]) = {
        val v = s.value

        v.fold(
          κ(result(
            expected.fold(κ(true), κ(false)),
            "both failed",
            s"$v is not $expected",
            s)),
            a => expected.fold(
              κ(result(false, "", "expected failure", s)),
              ex => result(a == ex, "both are equal", s"$a is not $ex", s)))
    }
  }

  def beFailing[E, A]: Matcher[Validation[E, A]] = new Matcher[Validation[E, A]] {
    def apply[S <: Validation[E, A]](s: Expectable[S]) = {
      val v = s.value

      result(
        v.fold(κ(true), κ(false)),
        s"$v is not success",
        s"$v is success",
        s
      )
    }
  }

  def beSuccessful[E, A](a: => A) = validationWith[E, A](Success(a))

  def beFailing[E, A](e: => E) = validationWith[E, A](Failure(e))

  def beFailureWithClass[E: ClassTag, A] = new Matcher[ValidationNel[E, A]] {
    def apply[S <: ValidationNel[E, A]](s: Expectable[S]) = {
      val v = s.value

      val requiredType = implicitly[ClassTag[E]].runtimeClass

      val rez = v match { case Failure(NonEmptyList(e)) => e.getClass == requiredType }

      result(rez, "v is a failure of " + requiredType, "v is not a failure of " + requiredType, s)
    }
  }

  private def validationWith[E, A](f: => Validation[E, A]): Matcher[Validation[E, A]] = new Matcher[Validation[E, A]] {
    def apply[S <: Validation[E, A]](s: Expectable[S]) = {
      val v = s.value

      val expected = f

      result(expected == v, s"$v is $expected", s"$v is not $expected", s)
    }
  }
}

object ValidationMatchers extends ValidationMatchers

trait DisjunctionMatchers {
  def beLeftDisjunction[A, B]: Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value

      result(v.fold(κ(true), κ(false)), s"$v is left", s"$v is not left", s)
    }
  }

  def beRightDisjunction[A, B]: Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value

      result(v.fold(κ(false), κ(true)), s"$v is right", s"$v is not right", s)
    }
  }

  def beRightDisjunction[A, B](p: B => Boolean)(implicit sb: Show[B]): Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value
      val vs = v.fold(a => a.toString(), b => sb.show(b))

      result(v.fold(κ(false), p), s"$vs is right", s"$vs is not right", s)
    }
  }

  def beRightDisjOrDiff[A, B](expected: B)(implicit rb: RenderTree[B]): Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value
      v.fold(
        a => result(false, s"$v is right", s"$v is not right", s),
        b => {
          val d = (b.render diff expected.render).shows
          result(b == expected,
            s"\n$v is right and tree matches:\n$d",
            s"\n$v is right but tree does not match:\n$d",
            s)
        })
    }
  }

  def beLeftDisjunction[A, B](p: A => Boolean)(implicit sa: Show[A]): Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value
      val vs = v.fold(a => sa.show(a), b => b.toString)

      result(v.fold(p, κ(true)), s"$vs is left", s"$vs is not left", s)
    }
  }

  def beRightDisjunction[A, B](expected: B)(implicit sb: Show[B]): Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value
      val vs = v.fold(a => a.toString(), b => sb.show(b))
      val exps = sb.show(expected)

      result(v.fold(κ(false), _ == expected), s"$vs is right $exps", s"$vs is not right $exps", s)
    }
  }

  def beLeftDisjunction[A, B](expected: A)(implicit sa: Show[A]): Matcher[A \/ B] = new Matcher[A \/ B] {
    def apply[S <: A \/ B](s: Expectable[S]) = {
      val v = s.value
      val vs = v.fold(a => sa.show(a), b => b.toString)
      val exps = sa.show(expected)

      result(v.fold(_ == expected, κ(false)), s"$vs is left $exps", s"$vs is not left $exps", s)
    }
  }
}
