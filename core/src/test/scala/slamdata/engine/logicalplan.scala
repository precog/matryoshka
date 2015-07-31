package slamdata.engine

import slamdata.Predef._
import slamdata.engine.fp._
import slamdata.engine.fs._

import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Arbitrary

import org.specs2.scalaz._
import scalaz._
import Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances._

class LogicalPlanSpecs extends Spec {
  import LogicalPlan._; import JoinType._

  implicit val arbLogicalPlan: Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[LogicalPlan[α]] =
        Arbitrary {
          Gen.oneOf(readGen[α], addGen(arb), constGen[α], joinGen(arb), letGen(arb), freeGen[α](Nil))
        }
    }

  // Switch this to parameterize over Funcs as well
  def addGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    l <- Arbitrary.arbitrary[A]
    r <- Arbitrary.arbitrary[A]
  } yield InvokeF(std.MathLib.Add, List(l, r))

  def letGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    n            <- Gen.choose(0, 1000)
    (form, body) <- Arbitrary.arbitrary[(A, A)]
  } yield LetF(Symbol("tmp" + n), form, body)

  def readGen[A]: Gen[LogicalPlan[A]] = Gen.const(ReadF(Path.Root))

  def joinGen[A: Arbitrary]: Gen[LogicalPlan[A]] = for {
    tpe <- Gen.oneOf(List(Inner, LeftOuter, RightOuter, FullOuter))
    (l, r, comp) <- Arbitrary.arbitrary[(A, A, A)]
  } yield JoinF(l, r, tpe, comp)

  import DataGen._

  def constGen[A]: Gen[LogicalPlan[A]] = for {
    data <- Arbitrary.arbitrary[Data]
  } yield ConstantF(data)

  def freeGen[A](vars: List[Symbol]): Gen[LogicalPlan[A]] = for {
    n <- Gen.choose(0, 1000)
  } yield FreeF(Symbol("tmp" + n))

  implicit val arbIntLP = arbLogicalPlan(Arbitrary.arbInt)

  checkAll(traverse.laws[LogicalPlan])
}
