package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.DisjunctionMatchers 

import scalaz._
import Scalaz._

import org.specs2.mutable._
import org.specs2.ScalaCheck

import org.scalacheck._
import Gen._

class PipelineSpec extends Specification with ScalaCheck with DisjunctionMatchers {
  def p(ops: PipelineOp*) = Pipeline(ops.toList)

  val empty = p()

  import PipelineOp._
  import ExprOp._

  implicit def arbitraryOp: Arbitrary[PipelineOp] = Arbitrary {
    def projectGen: Gen[PipelineOp] = for {
      c <- Gen.alphaChar
    } yield Project(Reshape(Map(c.toString -> -\/(Literal(Bson.Int32(1))))))

    def redactGen = for {
      value <- Gen.oneOf("$$DESCEND", "$$KEEP", "$$PRUNE")
    } yield Redact(Literal(Bson.Text(value)))

    def unwindGen = for {
      c <- Gen.alphaChar
    } yield Unwind(BsonField.Name(c.toString))
    
    def groupGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield Group(Grouped(Map("docsByAuthor" + i -> Sum(Literal(Bson.Int32(1))))), DocVar(BsonField.Name("author" + i)))
    
    def geoNearGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield GeoNear((40.0, -105.0), BsonField.Name("distance" + i), None, None, None, None, None, None, None)
    
    def outGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield Out(Collection("result" + i))
    
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    Gen.oneOf(
      projectGen,
      redactGen,
      (unwindGen ::
        groupGen ::
        geoNearGen ::
        outGen ::
        arbitraryShapePreservingOpGens): _*
    )
  }
  
  def arbitraryShapePreservingOp: Arbitrary[PipelineOp] = Arbitrary { 
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val gens = arbitraryShapePreservingOpGens
    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*) 
  }
    
  def arbitraryShapePreservingOpGens = {
    def matchGen: Gen[PipelineOp] = for {
      c <- Gen.alphaChar
    } yield Match(Selector.Doc(Map(BsonField.Name(c.toString) -> Selector.Eq(Bson.Int32(-1)))))

    def skipGen: Gen[PipelineOp] = for {
      i <- Gen.chooseNum(1, 10)
    } yield Skip(i)

    def limitGen: Gen[PipelineOp] = for {
      i <- Gen.chooseNum(1, 10)
    } yield Limit(i)

    def sortGen: Gen[PipelineOp] = for {
      c <- Gen.alphaChar
    } yield Sort(Map("name1" -> Ascending))
 
    List(matchGen, limitGen, skipGen, sortGen)
  }

  "Pipeline.merge" should {
    "return left when right is empty" ! prop { (p1: PipelineOp, p2: PipelineOp) =>
      val l = p(p1, p2)

      l.merge(empty) must (beRightDisj(l))
    }

    "return right when left is empty" ! prop { (p1: PipelineOp, p2: PipelineOp) =>
      val r = p(p1, p2)

      empty.merge(r) must (beRightDisj(r))
    }

    "return empty when both empty" in {
      empty.merge(empty) must (beRightDisj(empty))
    }

    "return left when left and right are equal" ! prop { (p1: PipelineOp, p2: PipelineOp) =>
      val v = p(p1, p2)

      v.merge(v) must (beRightDisj(v))
    }

    "merge any two shape-preserving ops from the left" ! (arbitraryShapePreservingOp, arbitraryShapePreservingOp) { (p1: PipelineOp, p2: PipelineOp) =>
      if (p1 == p2) {
        p(p1).merge(p(p2)) must beRightDisj(p(p1))
      }
      else {
        p(p1).merge(p(p2)) must beRightDisj(p(p1, p2))
        p(p2).merge(p(p1)) must beRightDisj(p(p2, p1))
      }
    }

    "merge two simple projections" in {
      val p1 = Project(Reshape(Map(
        "foo" -> -\/ (Literal(Bson.Int32(1)))
      )))

      val p2 = Project(Reshape(Map(
        "bar" -> -\/ (Literal(Bson.Int32(2)))
      )))   

      val r = Project(Reshape(Map(
        "foo" -> -\/ (Literal(Bson.Int32(1))),
        "bar" -> -\/ (Literal(Bson.Int32(2)))
      ))) 

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

     "merge two simple nested projections sharing top-level field name" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "baz" -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      val r = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9))),
          "baz" -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

    // "put match before project" in {
    //   val p1 = Project(Reshape(Map(
    //     "foo" -> \/- (Reshape(Map(
    //       "bar" -> -\/ (Literal(Bson.Int32(9)))
    //     )))
    //   )))
    //    
    //   val p2 = Match(Selector.Doc(Map(BsonField.Name("foo") -> Selector.Eq(Bson.Int32(-1)))))
    //    
    //   p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
    //   p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    // }

    "put redact before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Redact(DocVar(BsonField.Name("KEEP")))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    // "put limit before project" in {
    //   val p1 = Project(Reshape(Map(
    //     "foo" -> \/- (Reshape(Map(
    //       "bar" -> -\/ (Literal(Bson.Int32(9)))
    //     )))
    //   )))
    // 
    //   val p2 = Limit(10L)
    // 
    //   p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
    //   p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    // }

    "put any shape-preserving op before project" ! arbitraryShapePreservingOp { (p2: PipelineOp) =>
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put unwind before project" in {
      val p1 = Project(Reshape(Map(
        "foo" -> \/- (Reshape(Map(
          "bar" -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Unwind(BsonField.Name("foo"))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }
    
    "put $geoNear before any other (except another $geoNear)" ! prop { (p2: PipelineOp) =>
      val p1 = GeoNear((40.0, -105.0), BsonField.Name("distance"), None, None, None, None, None, None, None)

      p2 match {
        case GeoNear(_, _, _, _, _, _, _, _, _) if p1 == p2 => {
          p(p1).merge(p(p2)) must beRightDisj(p(p1))
          p(p2).merge(p(p1)) must beRightDisj(p(p1))
        }
        case GeoNear(_, _, _, _, _, _, _, _, _) => {
          p(p1).merge(p(p2)) must beLeftDisj
          p(p2).merge(p(p1)) must beLeftDisj
        }
        case _ => {
          p(p1).merge(p(p2)) must beRightDisj(p(p1, p2))
          p(p2).merge(p(p1)) must beRightDisj(p(p1, p2))
        }
      }
    }
    
    "put $out after any other (except another $out)" ! prop { (p2: PipelineOp) =>
      val p1 = Out(Collection("result"))

      p2 match {
        case Out(_) if p1 == p2 => {
          p(p1).merge(p(p2)) must beRightDisj(p(p1))
          p(p2).merge(p(p1)) must beRightDisj(p(p1))
        }
        case Out(_) => {
          p(p1).merge(p(p2)) must beLeftDisj
          p(p2).merge(p(p1)) must beLeftDisj
        }
        case _ => {
          p(p1).merge(p(p2)) must beRightDisj(p(p2, p1))
          p(p2).merge(p(p1)) must beRightDisj(p(p2, p1))
        }
      }
    }
    
    "merge any op with itself" ! prop { (op: PipelineOp) =>
      p(op).merge(p(op)) must beRightDisj(p(op))
    }
  }
}