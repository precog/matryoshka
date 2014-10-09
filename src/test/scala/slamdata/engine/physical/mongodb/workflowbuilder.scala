package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.execute.{Result}

import scala.collection.immutable.ListMap

import scalaz._, Scalaz._

import slamdata.engine.fp._

import slamdata.engine.{DisjunctionMatchers}
import slamdata.specs2._

class WorkflowBuilderSpec
    extends Specification
    with DisjunctionMatchers
    with PendingWithAccurateCoverage {
  import WorkflowTask._
  import WorkflowOp._
  import PipelineOp._

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = WorkflowBuilder.read(Collection("zips")).normalize

      op must_== readOp(Collection("zips"))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        city <- read.projectField("city").makeObject("city")
      } yield city.normalize

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))))))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        left   <- read.projectField("city").makeObject("city")
        right  <- read.projectField("pop").makeObject("pop")
        merged <- left objectConcat right
      } yield merged.normalize

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("pop"))))))))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val keys = read.projectField("city").makeArray
      val op = for {
        sort   <- read.sortBy(keys, Ascending :: Nil)
      } yield sort.normalize

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
            BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT())))),
          sortOp(
            NonEmptyList(
              BsonField.Name("lEft") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))))))
    }

    "merge unmergables" in {
      import Js._

      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        left   <- read.projectField("loc").projectIndex(1).makeObject("long")
        right  <- read.projectField("enemies").projectIndex(0).makeObject("public enemy #1")
        merged <- left objectConcat right
      } yield merged.normalize

      op must beRightDisjOrDiff(chain(
        foldLeftOp(
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("loc")))))),
            mapOp(
              MapOp.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(1, false)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> -\/(ExprOp.DocVar.ROOT()))))),
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("enemies")))))),
            mapOp(
              MapOp.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(0, false)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("rIght") -> -\/(ExprOp.DocVar.ROOT())))))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(ExprOp.DocField(BsonField.Name("lEft"))),
          BsonField.Name("public enemy #1") ->
            -\/(ExprOp.DocField(BsonField.Name("rIght"))))))))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        city   <- read.projectField("city").makeObject("city")
        dist   <- city.distinctBy(city)
      } yield dist.normalize

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
          groupOp(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
            \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))))),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))))))
    }
  }

  "distinct and sort with intervening op" in {
    val read = WorkflowBuilder.read(Collection("zips"))
    val op = for {
      city   <- read.projectField("city").makeObject("city")
      state  <- read.projectField("state").makeObject("state")
      projs  <- city objectConcat state
      
      key0   <- projs.projectField("city").makeObject("key")
      key1   <- projs.projectField("state").makeObject("key")
      keys   <- key0.makeArray arrayConcat key1.makeArray
      sorted <- projs.sortBy(keys, List(Ascending, Ascending))

      lim    = sorted >>> limitOp(10)  // Note: the compiler would not generate this op between sort and distinct

      dist   <- lim.distinctBy(lim)
    } yield dist.normalize

    println(op.map(_.show))

    op must beRightDisjOrDiff(chain(
        readOp(Collection("zips")),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))),
            BsonField.Name("state") -> -\/ (ExprOp.DocField(BsonField.Name("state")))))),
          BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
            BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
            BsonField.Index(1) -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("state")))))))))))),
        sortOp(NonEmptyList(
          BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending,
          BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key") -> Ascending)),
        limitOp(10),
        groupOp(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.First(ExprOp.DocField(BsonField.Name("lEft"))),
            BsonField.Name("__sd_key_0") -> ExprOp.First(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key"))),
            BsonField.Name("__sd_key_1") -> ExprOp.First(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key"))))),
          -\/ (ExprOp.DocVar.ROOT(BsonField.Name("lEft")))),
        sortOp(NonEmptyList(
          BsonField.Name("__sd_key_0") -> Ascending,
          BsonField.Name("__sd_key_1") -> Ascending)),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))),
          BsonField.Name("state") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("state"))))))))
  }.pendingUntilFixed("#378, but there are more intesting cases")

}
