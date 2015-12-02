package quasar

import quasar.Predef._
import quasar.fp._
import quasar.fs._

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._
import shapeless.contrib.scalaz.instances._

class LogicalPlanSpecs extends Spec {
  import LogicalPlan._

  implicit val arbLogicalPlan: Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]] =
    new (Arbitrary ~> λ[α => Arbitrary[LogicalPlan[α]]]) {
      def apply[α](arb: Arbitrary[α]): Arbitrary[LogicalPlan[α]] =
        Arbitrary {
          Gen.oneOf(readGen[α], addGen(arb), constGen[α], letGen(arb), freeGen[α](Nil))
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

  import DataGen._

  def constGen[A]: Gen[LogicalPlan[A]] = for {
    data <- Arbitrary.arbitrary[Data]
  } yield ConstantF(data)

  def freeGen[A](vars: List[Symbol]): Gen[LogicalPlan[A]] = for {
    n <- Gen.choose(0, 1000)
  } yield FreeF(Symbol("tmp" + n))

  implicit val arbIntLP = arbLogicalPlan(Arbitrary.arbInt)

  checkAll(traverse.laws[LogicalPlan])

  import quasar.recursionschemes.Fix
  import quasar.std.StdLib._, relations._, quasar.std.StdLib.set._, structural._

  "normalizeTempNames" should {
    "rename simple nested lets" in {
      LogicalPlan.normalizeTempNames(
        Let('foo, Read(Path.fileRel("foo")),
          Let('bar, Read(Path.fileRel("bar")),
            Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))),
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y"))))))))) must_==
        Let('__tmp0, Read(Path.fileRel("foo")),
          Let('__tmp1, Read(Path.fileRel("bar")),
            Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('__tmp0), Constant(Data.Str("x")))),
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('__tmp1), Constant(Data.Str("y"))))))))
    }

    "rename shadowed name" in {
      LogicalPlan.normalizeTempNames(
        Let('x, Read(Path.fileRel("foo")),
          Let('x, Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('x), Constant(Data.Str("x")))))),
            Free('x)))) must_==
        Let('__tmp0, Read(Path.fileRel("foo")),
          Let('__tmp1, Fix(MakeObjectN(
              Constant(Data.Str("x")) -> Fix(ObjectProject(Free('__tmp0), Constant(Data.Str("x")))))),
            Free('__tmp1)))
    }
  }

  "normalizeLets" should {
    "re-nest" in {
      LogicalPlan.normalizeLets(
        Let('bar,
          Let('foo,
            Read(Path.fileRel("foo")),
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x"))))))))),
          Fix(MakeObjectN(
            Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y")))))))) must_==
        Let('foo,
          Read(Path.fileRel("foo")),
          Let('bar,
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))))))),
            Fix(MakeObjectN(
              Constant(Data.Str("y")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("y"))))))))
    }

    "re-nest deep" in {
      LogicalPlan.normalizeLets(
        Let('baz,
          Let('bar,
            Let('foo,
              Read(Path.fileRel("foo")),
              Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Int(0))))))),
            Fix(Filter(Free('bar), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("y")))), Constant(Data.Int(1))))))),
          Fix(MakeObjectN(
            Constant(Data.Str("z")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("z")))))))) must_==
        Let('foo,
          Read(Path.fileRel("foo")),
          Let('bar,
            Fix(Filter(Free('foo), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("x")))), Constant(Data.Int(0)))))),
            Let('baz,
              Fix(Filter(Free('bar), Fix(Eq(Fix(ObjectProject(Free('foo), Constant(Data.Str("y")))), Constant(Data.Int(1)))))),
              Fix(MakeObjectN(
                Constant(Data.Str("z")) -> Fix(ObjectProject(Free('bar), Constant(Data.Str("z")))))))))
    }
  }
}
