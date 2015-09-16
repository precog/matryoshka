package quasar.physical.mongodb

import quasar.Predef._

import org.scalacheck._
import org.specs2.mutable._
import org.specs2.ScalaCheck
import scalaz._

import quasar._
import quasar.specs2._

class PipelineSpec extends Specification with ScalaCheck with ArbBsonField with PendingWithAccurateCoverage {
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import Workflow._
  import ArbitraryExprOp._

  implicit def arbitraryOp: Arbitrary[PipelineOp] = Arbitrary { Gen.resize(5, Gen.sized { size =>
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val ops = pipelineOpGens(size - 1)

    Gen.oneOf(ops(0), ops(1), ops.drop(2): _*)
  }) }

  def genProject(size: Int): Gen[$Project[Unit]] = for {
    fields <- Gen.nonEmptyListOf(for {
      c  <- Gen.alphaChar
      cs <- Gen.alphaStr

      field = c.toString + cs

      value <- if (size <= 0) genExpr.map(\/-(_))
      else Gen.oneOf(
        genProject(size - 1).map(p => -\/(p.shape)),
        genExpr.map(\/-(_)))
    } yield BsonField.Name(field) -> value)
    id <- Gen.oneOf(IdHandling.ExcludeId, IdHandling.IncludeId)
  } yield $Project((), Reshape(ListMap(fields: _*)), id)

  implicit def arbProject = Arbitrary[$Project[Unit]](Gen.resize(5, Gen.sized(genProject)))

  def genRedact = for {
    value <- Gen.oneOf($Redact.DESCEND, $Redact.KEEP, $Redact.PRUNE)
  } yield $Redact((), $var(value))

  def unwindGen = for {
    c <- Gen.alphaChar
  } yield $Unwind((), DocField(BsonField.Name(c.toString)))

  def genGroup = for {
    i <- Gen.chooseNum(1, 10)
  } yield $Group((),
    Grouped(ListMap(BsonField.Name("docsByAuthor" + i.toString) -> $sum($literal(Bson.Int32(1))))),
    \/-($var(DocField(BsonField.Name("author" + i)))))

  def genGeoNear = for {
    i <- Gen.chooseNum(1, 10)
  } yield $GeoNear((), (40.0, -105.0), BsonField.Name("distance" + i), None, None, None, None, None, None, None)

  def genOut = for {
    i <- Gen.chooseNum(1, 10)
  } yield $Out((), Collection("db", "result" + i))

  def pipelineOpGens(size: Int): List[Gen[PipelineOp]] = {
    genProject(size) ::
    genRedact ::
    unwindGen ::
    genGroup ::
    genGeoNear ::
    genOut ::
    arbitraryShapePreservingOpGens.map(g => for { sp <- g } yield sp.op)
  }

  case class ShapePreservingPipelineOp(op: PipelineOp)

  //implicit def arbitraryProject: Arbitrary[Project] = Arbitrary(genProject)

  implicit def arbitraryShapePreservingOp: Arbitrary[ShapePreservingPipelineOp] = Arbitrary {
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val gens = arbitraryShapePreservingOpGens
    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*)
  }

  def arbitraryShapePreservingOpGens = {
    def matchGen = for {
      c <- Gen.alphaChar
    } yield ShapePreservingPipelineOp($Match((), Selector.Doc(BsonField.Name(c.toString) -> Selector.Eq(Bson.Int32(-1)))))

    def skipGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield ShapePreservingPipelineOp($Skip((), i))

    def limitGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield ShapePreservingPipelineOp($Limit((), i))

    def sortGen = for {
      c <- Gen.alphaChar
    } yield ShapePreservingPipelineOp($Sort((), NonEmptyList(BsonField.Name("name1") -> Ascending)))

    List(matchGen, limitGen, skipGen, sortGen)
  }

  case class PairOfOpsWithSameType(op1: PipelineOp, op2: PipelineOp)

  implicit def arbitraryPair: Arbitrary[PairOfOpsWithSameType] = Arbitrary { Gen.resize(5, Gen.sized { size =>
    for {
      gen <- Gen.oneOf(pipelineOpGens(size))
      op1 <- gen
      op2 <- gen
    } yield PairOfOpsWithSameType(op1, op2)
  }) }

  "Project.id" should {
    "be idempotent" ! prop { (p: $Project[Unit]) =>
      p.id must_== p.id.id
    }
  }

  "Project.get" should {
    "retrieve whatever value it was set to" ! prop { (p: $Project[Unit], f: BsonField) =>
      val One = $literal(Bson.Int32(1))

      p.set(f, \/-(One)).get(DocVar.ROOT(f)) must (beSome(\/-(One)))
    }
  }

  "Project.setAll" should {
    "actually set all" ! prop { (p: $Project[Unit]) =>
      p.setAll(p.getAll.map(t => t._1 -> \/-(t._2))) must_== p
    }.pendingUntilFixed("result could have `_id -> _id` inserted without changing semantics")
  }

  "Project.deleteAll" should {
    "return empty when everything is deleted" ! prop { (p: $Project[Unit]) =>
      p.deleteAll(p.getAll.map(_._1)) must_== p.empty
    }
  }

  "SimpleMap.deleteAll" should {
    import jscore._

    "remove one un-nested field" in {
      val op = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> Select(ident("x"), "y"))))),
        ListMap())
      val exp = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b"))) must_== exp
    }

    "remove one nested field" in {
      val op = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "c" -> Select(ident("x"), "y"),
              "d" -> Select(ident("x"), "z")))))),
        ListMap())
      val exp = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "d" -> Select(ident("x"), "z")))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b") \ BsonField.Name("c"))) must_== exp
    }

    "remove whole nested object" in {
      val op = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"),
            "b" -> obj(
              "c" -> Select(ident("x"), "y")))))),
        ListMap())
      val exp = $SimpleMap(
        $read(Collection("db", "foo")),
        NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "a" -> Select(ident("x"), "x"))))),
        ListMap())
      op.deleteAll(List(BsonField.Name("b") \ BsonField.Name("c"))) must_== exp
    }
  }
}
