package slamdata.engine.physical.mongodb.accumulator

import collection.immutable.ListMap

import org.scalacheck._
import org.scalacheck.Arbitrary
import org.specs2.scalaz._
import scalaz._
import Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances.{deriveShow => _, _}

import slamdata.engine.fp._

class AccumulatorSpec extends Spec {
  implicit val arbAccumOp: Arbitrary ~> λ[α => Arbitrary[AccumOp[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[AccumOp[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[AccumOp[α]] =
        Arbitrary(arb.arbitrary.flatMap(a =>
          Gen.oneOf(
            $addToSet(a),
            $push(a),
            $first(a),
            $last(a),
            $max(a),
            $min(a),
            $avg(a),
            $sum(a))))
    }

  implicit val arbIntAccumOp = arbAccumOp(Arbitrary.arbInt)

  checkAll(traverse.laws[AccumOp])
}
