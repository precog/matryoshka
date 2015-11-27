package quasar
package fs

import quasar.Predef._

import scalaz._
import scalaz.Tags.{Multiplication => Mult}

final class Natural private (val run: Long) extends scala.AnyVal {
  def plus(other: Natural): Natural =
    new Natural(run + other.run)

  def + (other: Natural): Natural =
    plus(other)

  def times(other: Natural): Natural =
    new Natural(run * other.run)

  def * (other: Natural): Natural =
    times(other)

  def toInt: Int =
    run.toInt
}

object Natural {
  def apply(n: Long): Option[Natural] =
    Some(n).filter(_ >= 0).map(new Natural(_))

  val _0: Natural = new Natural(0)
  val _1: Natural = new Natural(1)
  val _2: Natural = new Natural(2)
  val _3: Natural = new Natural(3)
  val _4: Natural = new Natural(4)
  val _5: Natural = new Natural(5)
  val _6: Natural = new Natural(6)
  val _7: Natural = new Natural(7)
  val _8: Natural = new Natural(8)
  val _9: Natural = new Natural(9)

  def fromPositive(n: Positive): Natural =
    new Natural(n.run)

  implicit val naturalAddition: Monoid[Natural] =
    Monoid.instance(_ + _, _0)

  implicit val naturalMultiplication: Monoid[Natural @@ Mult] =
    Monoid.instance(
      (x, y) => Mult(Mult.unwrap(x) * Mult.unwrap(y)),
      Mult(_1))
}
