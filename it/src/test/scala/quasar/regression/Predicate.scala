package quasar
package regression

import quasar.Predef._

import argonaut._, Argonaut._

import scalaz.{Failure => _, _}
import scalaz.syntax.apply._
import scalaz.std.option._
import scalaz.stream._

import org.specs2.execute._
import org.specs2.matcher._

sealed trait Predicate {
  def apply[F[_]: Catchable: Monad](
    expected: Vector[Json],
    actual: Process[F, Json]
  ): F[Result]
}

object Predicate {
  import MustMatchers._
  import StandardResults._
  import DecodeResult.{ok => jok, fail => jfail}

  def matchJson(expected: Option[Json]): Matcher[Option[Json]] = new Matcher[Option[Json]] {
    def apply[S <: Option[Json]](s: Expectable[S]) = {
      (expected, s.value) match {
        case (Some(expected), Some(actual)) =>
          (actual.obj |@| expected.obj) { (actual, expected) =>
            if (actual.toList == expected.toList)
              success(s"matches $expected", s)
            else if (actual == expected)
              failure(s"$actual matches $expected, but order differs", s)
            else failure(s"$actual does not match $expected", s)
          } getOrElse result(actual == expected, s"matches $expected", s"$actual does not match $expected", s)
        case (Some(_), None)  => failure(s"ran out before expected", s)
        case (None, Some(v))  => failure(s"had more than expected: ${v}", s)
        case (None, None)     => success(s"matches (empty)", s)
        case _                => failure(s"scalac is weird", s)
      }
    }
  }

  /** Must contain ALL the elements in some order. */
  final case object ContainsAtLeast extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] =
      actual.scan(expected.toSet) { case (expected, e) =>
        expected.filterNot(jsonMatches(_, e))
      }
      .dropWhile(_.size > 0).take(1)
      .map(xs => xs aka "unmatched expected values" must beEmpty : Result)
      .runLastOr(failure)
  }

  /** Must contain ALL and ONLY the elements in some order. */
  final case object ContainsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] =
      actual.scan((expected.toSet, Set.empty[Json])) {
        case ((expected, extra), e) =>
          if (expected.contains(e))
            (expected.filterNot(jsonMatches(_, e)), extra)
          else
            (expected, extra + e)
        }
        .dropWhile(t => t._1.size > 0 && t._2.size == 0)
        .take(1)
        .map { case (exp, extra) =>
          (extra aka "unexpected values" must beEmpty) and
          (exp aka "unmatched expected values" must beEmpty): Result
        }
        .runLastOr(failure)
  }

  /** Must EXACTLY match the elements, in order. */
  final case object EqualsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual0: Process[F, Json]
    ): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))
      val zipped   = actual.tee(expected)(tee.zipAll(None, None))

      zipped flatMap { case ((a, e)) =>
        if (jsonMatches(a, e))
          Process.halt
        else
          Process.emit(a must matchJson(e) : Result)
      } take 1 runLastOr success
    }
  }

  /** Must START WITH the elements, in order. */
  final case object EqualsInitial extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual0: Process[F, Json]
    ): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))
      val zipped   = actual.tee(expected)(tee.zipAll(None, None))

      zipped flatMap {
        case (a, None) => Process.halt
        case (a, e) if (jsonMatches(a, e)) => Process.halt
        case (a, e) => Process.emit(a must matchJson(e) : Result)
      } take 1 runLastOr success
    }
  }

  /** Must NOT contain ANY of the elements. */
  final case object DoesNotContain extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] = {
      val expected = expected0.toSet

      actual.scan(expected) { case (exp, e) =>
        exp.filterNot(jsonMatches(_, e))
      }
      .dropWhile(_.size == expected.size)
      .take(1)
      .map(_ must_== expected : Result)
      .runLastOr(failure)
    }
  }

  private def jsonMatches(j1: Json, j2: Json): Boolean =
    (j1.obj.map(_.toList) |@| j2.obj.map(_.toList))(_ == _) getOrElse (j1 == j2)

  private def jsonMatches(j1: Option[Json], j2: Option[Json]): Boolean =
    (j1 |@| j2)(jsonMatches) getOrElse false

  implicit val PredicateDecodeJson: DecodeJson[Predicate] =
    DecodeJson(c => c.as[String].flatMap {
      case "containsAtLeast"  => jok(ContainsAtLeast)
      case "containsExactly"  => jok(ContainsExactly)
      case "doesNotContain"   => jok(DoesNotContain)
      case "equalsExactly"    => jok(EqualsExactly)
      case "equalsInitial"    => jok(EqualsInitial)
      case str                => jfail("Expected one of: containsAtLeast, containsExactly, doesNotContain, equalsExactly, equalsInitial, but found: " + str, c.history)
    })
}
