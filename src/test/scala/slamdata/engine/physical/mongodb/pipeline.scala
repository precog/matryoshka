package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
import slamdata.engine.DisjunctionMatchers 
import slamdata.engine.physical.mongodb.optimize._

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import org.specs2.mutable._
import org.specs2.ScalaCheck

import org.scalacheck._
import Gen._

class PipelineSpec extends Specification with ScalaCheck with DisjunctionMatchers with ArbBsonField {
  def p(ops: PipelineOp*) = Pipeline(ops.toList)

  val empty = p()

  import PipelineOp._
  import ExprOp._

  implicit def arbitraryOp: Arbitrary[PipelineOp] = Arbitrary { Gen.resize(5, Gen.sized { size =>
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    val ops = pipelineOpGens(size - 1)

    Gen.oneOf(ops(0), ops(1), ops.drop(2): _*)
  }) }

  lazy val genExpr: Gen[ExprOp] = Gen.const(Literal(Bson.Int32(1)))

  def genProject(size: Int): Gen[Project] = for {
    fields <- Gen.nonEmptyListOf(for {
                c  <- Gen.alphaChar
                cs <- Gen.alphaStr
                
                field = c.toString + cs

                value <- if (size <= 0) genExpr.map(-\/ apply) 
                         else Gen.oneOf(genExpr.map(-\/ apply), genProject(size - 1).map(p => \/- (p.shape)))
              } yield BsonField.Name(field) -> value)
  } yield Project(Reshape.Doc(ListMap(fields: _*)))

  implicit def arbProject = Arbitrary[Project](Gen.resize(5, Gen.sized(genProject)))

  def genRedact = for {
    value <- Gen.oneOf(Redact.DESCEND, Redact.KEEP, Redact.PRUNE)
  } yield Redact(value)

  def unwindGen = for {
    c <- Gen.alphaChar
  } yield Unwind(DocField(BsonField.Name(c.toString)))
  
  def genGroup = for {
    i <- Gen.chooseNum(1, 10)
  } yield Group(Grouped(ListMap(BsonField.Name("docsByAuthor" + i.toString) -> Sum(Literal(Bson.Int32(1))))), -\/(DocField(BsonField.Name("author" + i))))
  
  def genGeoNear = for {
    i <- Gen.chooseNum(1, 10)
  } yield GeoNear((40.0, -105.0), BsonField.Name("distance" + i), None, None, None, None, None, None, None)
  
  def genOut = for {
    i <- Gen.chooseNum(1, 10)
  } yield Out(Collection("result" + i))
  
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
    } yield ShapePreservingPipelineOp(Match(Selector.Doc(BsonField.Name(c.toString) -> Selector.Eq(Bson.Int32(-1)))))

    def skipGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield ShapePreservingPipelineOp(Skip(i))

    def limitGen = for {
      i <- Gen.chooseNum(1, 10)
    } yield ShapePreservingPipelineOp(Limit(i))

    def sortGen = for {
      c <- Gen.alphaChar
    } yield ShapePreservingPipelineOp(Sort(NonEmptyList(BsonField.Name("name1") -> Ascending)))
 
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
    "be idempotent" ! prop { (p: Project) =>
      p.id must_== p.id.id
    }
  }

  "Project.get" should {
    "retrieve whatever value it was set to" ! prop { (p: Project, f: BsonField) =>
      val One = ExprOp.Literal(Bson.Int32(1))

      p.set(f, -\/ (One)).get(DocVar.ROOT(f)) must (beSome(-\/ (One)))
    }
  }

  "Project.setAll" should {
    "actually set all" ! prop { (p: Project) =>
      p.setAll(p.getAll.map(t => t._1 -> -\/ (t._2))) must_== p
    }
  }

  "Project.deleteAll" should {
    "return empty when everything is deleted" ! prop { (p: Project) =>
      p.deleteAll(p.getAll.map(_._1)) must_== p.empty
    }
  }
  
  "ExprOp" should {

    "escape literal string with $" in {
      Literal(Bson.Text("$1")).bson must_== Bson.Doc(ListMap("$literal" -> Bson.Text("$1")))
    }

    "escape literal string with no leading '$'" in {
      val x = Bson.Text("abc")
      Literal(x).bson must_== Bson.Doc(ListMap("$literal" -> Bson.Text("abc")))
    }

    "escape simple integer literal" in {
      val x = Bson.Int32(0)
      Literal(x).bson must_== Bson.Doc(ListMap("$literal" -> Bson.Int32(0)))
    }

    "escape simple array literal" in {
      val x = Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)
      Literal(x).bson must_== Bson.Doc(ListMap("$literal" -> Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)))
    }

    "escape string nested in array" in {
      val x = Bson.Arr(Bson.Text("$1") :: Nil)
      val exp = Bson.Doc(ListMap("$literal" -> x))
      Literal(x).bson must_== exp
    }

    "escape simple doc literal" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("b")))
      Literal(x).bson must_== Bson.Doc(ListMap("$literal" -> x))
    }

    "escape string nested in doc" in {
      val x = Bson.Doc(ListMap("a" -> Bson.Text("$1")))
      val exp = Bson.Doc(ListMap("$literal" -> x))
      Literal(x).bson must_== exp
    }

    "render $$ROOT" in {
      DocVar.ROOT().bson.repr must_== "$$ROOT"
    }

    "treat DocField as alias for DocVar.ROOT()" in {
      DocField(BsonField.Name("foo")) must_== DocVar.ROOT(BsonField.Name("foo"))
    }
    
    "render $foo under $$ROOT" in {
      DocVar.ROOT(BsonField.Name("foo")).bson.repr must_== "$foo"
    }

    "render $foo.bar under $$CURRENT" in {
      DocVar.CURRENT(BsonField.Name("foo") \ BsonField.Name("bar")).bson.repr must_== "$$CURRENT.foo.bar"
    }

    "render $redact result variables" in {
      Redact.DESCEND.bson.repr must_== "$$DESCEND"
      Redact.PRUNE.bson.repr   must_== "$$PRUNE"
      Redact.KEEP.bson.repr    must_== "$$KEEP"
    }

  }
}
