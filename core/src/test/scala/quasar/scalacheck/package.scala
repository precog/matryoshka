package quasar

import quasar.Predef._

import org.scalacheck.{Gen, Arbitrary}
import scalaz.{Apply, NonEmptyList}
import scalaz.scalacheck.ScalaCheckBinding._

package object scalacheck {
  def nonEmptyListSmallerThan[A: Arbitrary](n: Int): Arbitrary[NonEmptyList[A]] = {
    val listGen = Gen.containerOfN[List,A](n, implicitly[Arbitrary[A]].arbitrary)
    Apply[Arbitrary].apply2[A, List[A], NonEmptyList[A]](implicitly[Arbitrary[A]], Arbitrary(listGen))(NonEmptyList.nel)
  }

  def listSmallerThan[A: Arbitrary](n: Int): Arbitrary[List[A]] =
    Arbitrary(Gen.containerOfN[List,A](n,implicitly[Arbitrary[A]].arbitrary))
}
