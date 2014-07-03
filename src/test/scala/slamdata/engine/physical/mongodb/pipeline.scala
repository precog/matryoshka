package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.fp._
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
    // Note: Gen.oneOf is overridden and this variant requires two explicit args
    Gen.oneOf(opGens(0), opGens(1), opGens.drop(2): _*)
  }

  def projectGen: Gen[PipelineOp] = for {
    c <- Gen.alphaChar
  } yield Project(Reshape.Doc(Map(BsonField.Name(c.toString) -> -\/(Literal(Bson.Int32(1))))))

  def redactGen = for {
    value <- Gen.oneOf(Redact.DESCEND, Redact.KEEP, Redact.PRUNE)
  } yield Redact(value)

  def unwindGen = for {
    c <- Gen.alphaChar
  } yield Unwind(DocField(BsonField.Name(c.toString)))
  
  def groupGen = for {
    i <- Gen.chooseNum(1, 10)
  } yield Group(Grouped(Map(BsonField.Name("docsByAuthor" + i.toString) -> Sum(Literal(Bson.Int32(1))))), -\/(DocField(BsonField.Name("author" + i))))
  
  def geoNearGen = for {
    i <- Gen.chooseNum(1, 10)
  } yield GeoNear((40.0, -105.0), BsonField.Name("distance" + i), None, None, None, None, None, None, None)
  
  def outGen = for {
    i <- Gen.chooseNum(1, 10)
  } yield Out(Collection("result" + i))
  
  def opGens = {
    projectGen ::
      redactGen ::
      unwindGen ::
      groupGen ::
      geoNearGen ::
      outGen ::
      arbitraryShapePreservingOpGens.map(g => for { sp <- g } yield sp.op)
  }
  
  case class ShapePreservingPipelineOp(op: PipelineOp)

  //implicit def arbitraryProject: Arbitrary[Project] = Arbitrary(projectGen)
  
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
  
  implicit def arbitraryPair: Arbitrary[PairOfOpsWithSameType] = Arbitrary {  
    for {
      gen <- Gen.oneOf(opGens)
      op1 <- gen
      op2 <- gen
    } yield PairOfOpsWithSameType(op1, op2)
  }
      
  "MergePatch.Id" should {
    "do nothing with pipeline op" in {
      MergePatch.Id(Skip(10)).map(_._1) must (beRightDisj(List[PipelineOp](Skip(10))))
    }

    "return Id for successor patch" in {
      MergePatch.Id(Skip(10)).map(_._2) must (beRightDisj(MergePatch.Id))
    }
  }

  "MergePatch.Rename" should {
    "rename top-level field" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename top-level field defined by ROOT doc var" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.ROOT(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.ROOT(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename top-level field defined by CURRENT doc var" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.CURRENT(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocVar.CURRENT(BsonField.Name("buz")))
      ))))

      val applied = MergePatch.Rename(DocVar.CURRENT(BsonField.Name("baz")), DocVar.CURRENT(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename even root fields when ROOT is renamed" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("baz")))
      )))

      val expect = List[PipelineOp](Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/ (DocField(BsonField.Name("buz") \ BsonField.Name("baz")))
      ))))

      val applied = MergePatch.Rename(DocVar.ROOT(), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(expect))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }
    
    "not rename nested" in {
      val init = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/(DocField(BsonField.Name("foo") \ BsonField.Name("baz")))
      )))

      val applied = MergePatch.Rename(DocField(BsonField.Name("baz")), DocField(BsonField.Name("buz")))(init)

      applied.map(_._1) must (beRightDisj(List[PipelineOp](init)))
      applied.map(_._2) must (beRightDisj(MergePatch.Id))
    }

    "rename Then sequentially" in {
      import Selector._

      val op = Match(
        Selector.Doc(Map[BsonField, SelectorExpr](
          BsonField.Name("name")      -> Selector.Expr(Selector.Eq(Bson.Text("Steve"))), 
          BsonField.Name("age")       -> Selector.Expr(Selector.Gt(Bson.Int32(18))), 
          BsonField.Name("length")    -> Selector.Expr(Selector.Lte(Bson.Dec(8.5))), 
          BsonField.Name("publisher") -> Selector.Expr(Selector.Neq(Bson.Text("Amazon")))
        ))
      )

      val patch = 
        MergePatch.Rename(DocVar.ROOT(BsonField.Name("name")), DocVar.ROOT(BsonField.Name("__sd_tmp_1"))) >>
        MergePatch.Rename(DocVar.ROOT(BsonField.Name("length")), DocVar.ROOT(BsonField.Name("__sd_tmp_2")))

      val expect = Match(
        Selector.Doc(Map[BsonField, SelectorExpr](
          BsonField.Name("__sd_tmp_1")  -> Selector.Expr(Selector.Eq(Bson.Text("Steve"))), 
          BsonField.Name("age")         -> Selector.Expr(Selector.Gt(Bson.Int32(18))), 
          BsonField.Name("__sd_tmp_2")  -> Selector.Expr(Selector.Lte(Bson.Dec(8.5))), 
          BsonField.Name("publisher")   -> Selector.Expr(Selector.Neq(Bson.Text("Amazon")))
        ))
      )

      patch(op) must (beRightDisj((expect :: Nil) -> patch))
    }
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

    "be deterministic regardless of parameter order" ! prop { (p1: PipelineOp, p2: PipelineOp) =>
      val pl1 = p(p1)
      val pl2 = p(p2)
  
      pl1.merge(pl2) must_== pl2.merge(pl1)
    }

    "merge two ops of same type, unless an error" ! prop { (ps: PairOfOpsWithSameType) =>
      val pl1 = p(ps.op1)
      val pl2 = p(ps.op2)

      val mergedOps = pl1.merge(pl2).map(_.ops)
      mergedOps.fold(
        e => 1 must_== 1,  // HACK: ok, nothing to check if an error
        ops => ops must have length(1)  // TODO: ... and should have the same type as both ops
      )
    }.pendingUntilFixed

    "merge two simple projections" in {
      val p1 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> -\/ (Literal(Bson.Int32(1)))
      )))

      val p2 = Project(Reshape.Doc(Map(
        BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(2)))
      )))   

      val r = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> -\/ (Literal(Bson.Int32(1))),
        BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(2)))
      ))) 

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

     "merge two simple nested projections sharing top-level field name" in {
      val p1 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("baz") -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      val r = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(9))),
          BsonField.Name("baz") -> -\/ (Literal(Bson.Int32(2)))
        )))
      )))

      p(p1).merge(p(p2)) must (beRightDisj(p(r)))
    }

    "put redact before project" in {
      val p1 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Redact(Redact.KEEP)

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put any shape-preserving op before project" ! prop { (sp: ShapePreservingPipelineOp) =>
      val p1 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))
      val p2 = sp.op
      
      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put unwind before project" in {
      val p1 = Project(Reshape.Doc(Map(
        BsonField.Name("foo") -> \/- (Reshape.Doc(Map(
          BsonField.Name("bar") -> -\/ (Literal(Bson.Int32(9)))
        )))
      )))

      val p2 = Unwind(DocField(BsonField.Name("foo")))

      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }
    
    "put any shape-preserving op before unwind" ! prop { (sp: ShapePreservingPipelineOp) =>
      val p1 = Unwind(DocField(BsonField.Name("foo")))
      val p2 = sp.op
      
      p(p1).merge(p(p2)) must (beRightDisj(p(p2, p1)))
      p(p2).merge(p(p1)) must (beRightDisj(p(p2, p1)))
    }

    "put any shape-preserving op before redact" ! prop { (sp: ShapePreservingPipelineOp) =>
      val p1 = Redact(Redact.KEEP)
      val p2 = sp.op
      
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
          p(p1).merge(p(p2)) must beAnyLeftDisj
          p(p2).merge(p(p1)) must beAnyLeftDisj
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
          p(p1).merge(p(p2)) must beAnyLeftDisj
          p(p2).merge(p(p1)) must beAnyLeftDisj
        }
        case _ => {
          p(p1).merge(p(p2)) must beRightDisj(p(p2, p1))
          p(p2).merge(p(p1)) must beRightDisj(p(p2, p1))
        }
      }
    }
    
    "merge any op with itself" ! prop { (op: PipelineOp) =>
      p(op).merge(p(op)) must beRightDisj(p(op))
      }.pendingUntilFixed
    
    "merge skips with min" in {
      val p1 = p(Skip(5))
      val p2 = p(Skip(10))
      
      p1.merge(p2) must beRightDisj(p(Skip(5)))
      p2.merge(p1) must beRightDisj(p(Skip(5)))
    }
    
    "merge limits with max" in {
      val p1 = p(Limit(5))
      val p2 = p(Limit(10))
      
      p1.merge(p2) must beRightDisj(p(Limit(10)))
      p2.merge(p1) must beRightDisj(p(Limit(10)))
    }
    
    "merge skip and limit" in {
      val p1 = p(Skip(5))
      val p2 = p(Limit(10))
      
      val exp = p(Limit(5), Skip(5))
      
      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }
    
    "merge skip and limit (empty)" in {
      val p1 = p(Skip(15))
      val p2 = p(Limit(10))
      
      // Just Limit(0) would work as well; anyway nothing's coming back
      val exp = p(Limit(0), Skip(15))
      
      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }
    
    "merge skip/limit before match" in {
      val p1 = p(Skip(5), Limit(10))
      val p2 = p(Match(Selector.Doc(BsonField.Name("foo") -> Selector.Eq(Bson.Int32(-1)))))
      
      p1.merge(p2) must beRightDisj(Pipeline(p1.ops ++ p2.ops))
      p2.merge(p1) must beRightDisj(Pipeline(p1.ops ++ p2.ops))
    }
    
    "merge match before sort" in {
      val p1 = p(Match(Selector.Doc(BsonField.Name("foo") -> Selector.Eq(Bson.Int32(-1)))))
      val p2 = p(Sort(NonEmptyList(BsonField.Name("bar") -> Ascending)))
      
      p1.merge(p2) must beRightDisj(Pipeline(p1.ops ++ p2.ops))
      p2.merge(p1) must beRightDisj(Pipeline(p1.ops ++ p2.ops))
    }
    
    "merge matches with different keys" in {
      val sel1 = BsonField.Name("foo") -> Selector.Eq(Bson.Int32(1))
      val sel2 = BsonField.Name("bar") -> Selector.Eq(Bson.Int32(2))
      val p1 = p(Match(Selector.Doc(sel1)))
      val p2 = p(Match(Selector.Doc(sel2)))
      val exp = p(Match(Selector.Doc(sel1, sel2)))
      
      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }
    
    "merge matches with same key" in {
      val sel1 = Selector.Gt(Bson.Int32(5))
      val sel2 = Selector.Lt(Bson.Int32(10))
      val p1 = p(Match(Selector.Doc(BsonField.Name("foo") -> sel1)))
      val p2 = p(Match(Selector.Doc(BsonField.Name("foo") -> sel2)))
      val exp1 = p(Match(Selector.And(
        Selector.Doc(BsonField.Name("foo") -> sel1),
        Selector.Doc(BsonField.Name("foo") -> sel2)
      )))
      val exp2 = p(Match(Selector.And(
        Selector.Doc(BsonField.Name("foo") -> sel2),
        Selector.Doc(BsonField.Name("foo") -> sel1)
      )))

      // Note: Note: Selector.And equality does not ignore order of its children
      p1.merge(p2) must beRightDisj(exp1)
      p2.merge(p1) must beRightDisj(exp2)
    }
    
    "merge redact before unrelated unwind" in {
      val op1 = Redact(Redact.PRUNE)
      val op2 = Unwind(DocField(BsonField.Name("foo")))
      
      p(op1).merge(p(op2)) must beRightDisj(p(op1, op2))
      p(op2).merge(p(op1)) must beRightDisj(p(op1, op2))
    }
    
    "fail to merge redact with unwind referencing same name" in {
      val op1 = Redact(
                  Cond(
                    Gt(
                      Size(
                        SetIntersection(
                          DocField(BsonField.Name("tags")),
                          Literal(Bson.Arr(Bson.Text("STLW") :: Bson.Text("G") :: Nil))
                        )
                      ), 
                      Literal(Bson.Int32(0))
                    ), 
                    Redact.PRUNE, 
                    Redact.KEEP)
                  )
      val op2 = Unwind(DocField(BsonField.Name("tags")))
      
      p(op1).merge(p(op2)) must beAnyLeftDisj
      p(op2).merge(p(op1)) must beAnyLeftDisj
    }

    "merge multiple unwinds lexically" in {
      val op1 = Unwind(DocField(BsonField.Name("foo")))
      val op2 = Unwind(DocField(BsonField.Name("bar")))
      
      p(op1).merge(p(op2)) must beRightDisj(p(op2, op1))
      p(op2).merge(p(op1)) must beRightDisj(p(op2, op1))
    }
    
    "merge unrelated Projects" in {
      val p1 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("title") -> -\/ (DocField(BsonField.Name("title")))
                  )))
                )
      val p2 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("author") -> -\/ (DocField(BsonField.Name("author")))
                  )))
                )
     
      val exp = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("title") -> -\/ (DocField(BsonField.Name("title"))),
                    BsonField.Name("author") -> -\/ (DocField(BsonField.Name("author")))
                  )))
                )

      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }
    
    "merge conflicting Projects" in {
      val p1 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("name") -> -\/ (DocField(BsonField.Name("title"))),
                    BsonField.Name("length") -> -\/ (DocField(BsonField.Name("pageCount"))),
                    BsonField.Name("publisher") -> -\/ (DocField(BsonField.Name("publisher")))
                  )))
                )
      val p2 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("name") -> -\/ (DocField(BsonField.Name("author"))),         // conflicts
                    BsonField.Name("age") -> -\/ (DocField(BsonField.Name("age"))),             // this side only 
                    BsonField.Name("length") -> -\/ (DocField(BsonField.Name("dimensions") \ BsonField.Name("length"))),  // conflicts
                    BsonField.Name("publisher") -> -\/ (DocField(BsonField.Name("publisher")))  // matches on both sides
                  ))),
                  Match(Selector.Doc(
                    BsonField.Name("name") -> Selector.Eq(Bson.Text("Steve")),
                    BsonField.Name("age") -> Selector.Gt(Bson.Int32(18)),
                    BsonField.Name("length") -> Selector.Lte(Bson.Dec(8.5)),
                    BsonField.Name("publisher") -> Selector.Neq(Bson.Text("Amazon"))
                  ))
                )
     
      // This result assumes the merge renames variables on the right:
      val exp = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("name") -> -\/ (DocField(BsonField.Name("title"))),
                    BsonField.Name("length") -> -\/ (DocField(BsonField.Name("pageCount"))),
                    BsonField.Name("__sd_tmp_1") -> -\/ (DocField(BsonField.Name("author"))),
                    BsonField.Name("age") -> -\/ (DocField(BsonField.Name("age"))),
                    BsonField.Name("__sd_tmp_2") -> -\/ (DocField(BsonField.Name("dimensions") \ BsonField.Name("length"))),
                    BsonField.Name("publisher") -> -\/ (DocField(BsonField.Name("publisher")))
                  ))),
                  Match(Selector.Doc(
                    BsonField.Name("__sd_tmp_1") -> Selector.Eq(Bson.Text("Steve")),
                    BsonField.Name("age") -> Selector.Gt(Bson.Int32(18)),
                    BsonField.Name("__sd_tmp_2") -> Selector.Lte(Bson.Dec(8.5)),
                    BsonField.Name("publisher") -> Selector.Neq(Bson.Text("Amazon"))
                  ))
                )

      p1.merge(p2) must beRightDisj(exp)
      // opposite merge just renames the other variable
    }

    "merge conflicting nested Projects (flattened)" in {
      val p1 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("author")    -> \/- (Reshape.Doc(Map(
                      BsonField.Name("name")      -> -\/ (DocField(BsonField.Name("author"))),
                      BsonField.Name("city")      -> -\/ (DocField(BsonField.Name("authorCity")))
                    ))),
                    BsonField.Name("length")    -> -\/ (DocField(BsonField.Name("pageCount"))),
                    BsonField.Name("publisher") -> -\/ (DocField(BsonField.Name("publisherName")))
                  )))
                )
      val p2 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("author")    -> \/- (Reshape.Doc(Map(
                      BsonField.Name("name")      -> -\/ (DocField(BsonField.Name("authorFullName"))), // conflicts
                      BsonField.Name("city")      -> -\/ (DocField(BsonField.Name("authorCity"))),     // same
                      BsonField.Name("age")       -> -\/ (DocField(BsonField.Name("authorAge")))        // this side only
                    ))),
                    BsonField.Name("length")    -> -\/ (DocField(BsonField.Name("dimensions") \ BsonField.Name("length"))), // conflicts
                    BsonField.Name("publisher") -> \/- (Reshape.Doc(Map(
                      BsonField.Name("name")      -> -\/ (DocField(BsonField.Name("publisherName")))  // shape conflicts
                    )))
                  ))),
                  Match(Selector.Doc(
                    BsonField.Name("author") \ BsonField.Name("name")    -> Selector.Eq(Bson.Text("Steve")),
                    BsonField.Name("age")                                -> Selector.Gt(Bson.Int32(18)),
                    BsonField.Name("length")                             -> Selector.Lte(Bson.Dec(8.5)),
                    BsonField.Name("publisher") \ BsonField.Name("name") -> Selector.Neq(Bson.Text("Amazon"))
                  ))
                )
     
      // This result assumes the merge renames variables on the right:
      val exp = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("author")     -> \/- (Reshape.Doc(Map(
                      BsonField.Name("name")       -> -\/ (DocField(BsonField.Name("author"))),
                      BsonField.Name("city")       -> -\/ (DocField(BsonField.Name("authorCity"))),
                      BsonField.Name("__sd_tmp_1") -> -\/ (DocField(BsonField.Name("authorFullName"))),
                      BsonField.Name("age")        -> -\/ (DocField(BsonField.Name("authorAge")))
                    ))),
                    BsonField.Name("length")     -> -\/ (DocField(BsonField.Name("pageCount"))),
                    BsonField.Name("__sd_tmp_1") -> -\/ (DocField(BsonField.Name("dimensions") \ BsonField.Name("length"))),
                    BsonField.Name("publisher")  -> -\/ (DocField(BsonField.Name("publisherName"))),
                    BsonField.Name("__sd_tmp_2") -> \/- (Reshape.Doc(Map(
                      BsonField.Name("name")       -> -\/ (DocField(BsonField.Name("publisherName")))
                    )))
                  ))),
                  Match(Selector.Doc(
                    BsonField.Name("author") \ BsonField.Name("__sd_tmp_1") -> Selector.Eq(Bson.Text("Steve")),
                    BsonField.Name("age")                                   -> Selector.Gt(Bson.Int32(18)),
                    BsonField.Name("__sd_tmp_1")                            -> Selector.Lte(Bson.Dec(8.5)),
                    BsonField.Name("__sd_tmp_2") \ BsonField.Name("name")   -> Selector.Neq(Bson.Text("Amazon"))
                  ))
                )

      p1.merge(p2) must beRightDisj(exp)
      // opposite merge just renames the other variable
    }
    
    "merge group with project" in {
      val p1 = p(
                Project(Reshape.Doc(Map(
                  BsonField.Name("title") -> -\/ (DocField(BsonField.Name("title")))
                )))
              )
      val p2 = p(
                Group(
                  Grouped(Map(
                    BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1)))
                  )), 
                  -\/(DocField(BsonField.Name("author")))
                ),
                Project(Reshape.Doc(Map(BsonField.Name("docsByAuthor") -> -\/ (DocField(BsonField.Name("docsByAuthor"))))))
              )
     
      val exp = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1))),
                      BsonField.Name("__sd_tmp_1") -> Push(DocVar.ROOT())
                    )),
                    -\/(DocField(BsonField.Name("author")))
                  ),
                  Unwind(DocField(BsonField.Name("__sd_tmp_1"))),
                  Project(Reshape.Doc(Map(
                          BsonField.Name("title") -> -\/ (DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("title"))),
                          BsonField.Name("docsByAuthor") -> -\/ (DocField(BsonField.Name("docsByAuthor")))
                  )))
                )

      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }
    
    "merge group with related project" in {
      val p1 = p(
                  Project(Reshape.Doc(Map(
                    BsonField.Name("author") -> -\/ (DocVar(DocVar.Name("author"), None))
                  )))
                )
      val p2 = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1)))
                    )),
                    -\/(DocVar(DocVar.Name("author"), None))
                  ),
                  Project(Reshape.Doc(Map(
                    BsonField.Name("docsByAuthor") -> -\/ (DocVar(DocVar.Name("docsByAuthor"), None))
                  )))
                )
      
      // Here's an efficient implementation--pull the grouped-on field from the key:
      val exp = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1)))
                    )),
                    -\/(DocField(BsonField.Name("author")))
                  ),
                  Project(Reshape.Doc(Map(
                          BsonField.Name("author") -> -\/ (DocField(BsonField.Name("_id") \ BsonField.Name("author"))),
                          BsonField.Name("docsByAuthor") -> -\/ (DocField(BsonField.Name("docsByAuthor")))
                  )))
                )
      
      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }.pendingUntilFixed

    "merge group with unwind" in {
      val p1 = p(
                  Unwind(DocField(BsonField.Name("tags"))), 
                  Match(Selector.Doc(BsonField.Name("tags") -> Selector.Eq(Bson.Text("new"))))
                )
      val p2 = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1)))
                    )),
                    -\/(DocField(BsonField.Name("author")))
                  )
                )
      
      val exp = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1))),
                      BsonField.Name("__sd_tmp_1") -> Push(DocField(BsonField.Name("tags")))
                    )),
                    -\/(DocField(BsonField.Name("author")))
                  ),
                  Unwind(DocField(BsonField.Name("__sd_tmp_1"))),
                  Unwind(DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("tags"))),
                  Match(Selector.Doc(BsonField.Name("__sd_tmp_1") \ BsonField.Name("tags") -> Selector.Eq(Bson.Text("new"))))
                )
          
      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }.pendingUntilFixed

    "merge Group with unrelated Redact" in {
      val p1 = p(
                Redact(
                  Cond(
                    Eq(
                      DocField(BsonField.Name("private")),
                      Literal(Bson.Bool(true))
                    ),
                    Redact.PRUNE,
                    Redact.KEEP
                  )
                )
              )
      val p2 = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthor") -> Sum(Literal(Bson.Int32(1)))
                    )),
                    -\/(DocField(BsonField.Name("author")))
                  )
                )

      // In this case, the redaction doesn't affect the grouping key, so it can be done first:
      val exp = Pipeline(p1.ops ++ p2.ops)

      p1.merge(p2) must beRightDisj(exp)
      p2.merge(p1) must beRightDisj(exp)
    }.pendingUntilFixed

    "merge Group with conflicting Redact" in {
      val p1 = p(
                Redact(   // how to eliminate the "author" sub-document but keep the rest?
                  Cond(
                    ???, ???, ???
                  )
                )
              )
      val p2 = p(
                  Group(
                    Grouped(Map(
                      BsonField.Name("docsByAuthorName") -> Sum(Literal(Bson.Int32(1)))
                    )),
                    -\/(DocField(BsonField.Name("author") \ BsonField.Name("name")))
                  )
                )

      val exp = ???

      p1.merge(p2) must beAnyLeftDisj
      p2.merge(p1) must beAnyLeftDisj
    }.pendingUntilFixed
    
  }
  
  "ExprOp" should {

    "escape literal string with $" in {
      Literal(Bson.Text("$1")).bson must_== Bson.Doc(Map("$literal" -> Bson.Text("$1")))
    }

    "not escape literal string with no leading '$'" in {
      val x = Bson.Text("abc")
      Literal(x).bson must_== x
    }

    "not escape simple integer literal" in {
      val x = Bson.Int32(0)
      Literal(x).bson must_== x
    }

    "not escape simple array literal" in {
      val x = Bson.Arr(Bson.Text("abc") :: Bson.Int32(0) :: Nil)
      Literal(x).bson must_== x
    }

    "escape string nested in array" in {
      val x = Bson.Arr(Bson.Text("$1") :: Nil)
      val exp = Bson.Arr(Bson.Doc(Map("$literal" -> Bson.Text("$1"))) :: Nil)
      Literal(x).bson must_== exp
    }

    "not escape simple doc literal" in {
      val x = Bson.Doc(Map("a" -> Bson.Text("b")))
      Literal(x).bson must_== x
    }

    "escape string nested in doc" in {
      val x = Bson.Doc(Map("a" -> Bson.Text("$1")))
      val exp = Bson.Doc(Map("a" -> Bson.Doc(Map("$literal" -> Bson.Text("$1")))))
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
