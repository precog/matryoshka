package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Positive private (val run: Long) extends scala.AnyVal {
  def plus(other: Positive): Positive =
    new Positive(run + other.run)

  def + (other: Positive): Positive =
    plus(other)

  def times(other: Positive): Positive =
    new Positive(run * other.run)

  def * (other: Positive): Positive =
    times(other)

  def toInt: Int =
    run.toInt
}

object Positive {
  def apply(n: Long): Option[Positive] =
    Some(n).filter(_ > 0).map(new Positive(_))

  val _1: Positive = new Positive(1)
  val _2: Positive = new Positive(2)
  val _3: Positive = new Positive(3)
  val _4: Positive = new Positive(4)
  val _5: Positive = new Positive(5)
  val _6: Positive = new Positive(6)
  val _7: Positive = new Positive(7)
  val _8: Positive = new Positive(8)
  val _9: Positive = new Positive(9)

  implicit val positiveSemigroup: Semigroup[Positive] =
    Semigroup.instance(_ + _)

  implicit val positiveMultiplication: Monoid[Positive @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(_1))
}
