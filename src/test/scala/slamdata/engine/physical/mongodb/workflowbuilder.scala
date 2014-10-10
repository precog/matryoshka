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
  import WorkflowOp._
  import PipelineOp._
  import IdHandling._

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = WorkflowBuilder.read(Collection("zips")).build

      op must_== readOp(Collection("zips"))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        city <- read.projectField("city").makeObject("city")
      } yield city.build

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))),
            IgnoreId)))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        left   <- read.projectField("city").makeObject("city")
        right  <- read.projectField("pop").makeObject("pop")
        merged <- left objectConcat right
      } yield merged.build

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("pop"))))),
            IgnoreId)))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val keys = read.projectField("city").makeArray
      val op = for {
        sort   <- read.sortBy(keys, Ascending :: Nil)
      } yield sort.build

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
            BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT()))),
            IncludeId),
          sortOp(
            NonEmptyList(
              BsonField.Name("lEft") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending)),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("rIght"))))),
            ExcludeId)))
    }

    "merge unmergables" in {
      import Js._

      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        left   <- read.projectField("loc").projectIndex(1).makeObject("long")
        right  <- read.projectField("enemies").projectIndex(0).makeObject("public enemy #1")
        merged <- left objectConcat right
      } yield merged.build

      op must beRightDisjOrDiff(chain(
        foldLeftOp(
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("loc"))))),
              IgnoreId),
            mapOp(
              MapOp.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(1, false)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("lEft") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)),
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("enemies"))))),
              IgnoreId),
            mapOp(
              MapOp.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(0, false)))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("rIght") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(ExprOp.DocField(BsonField.Name("lEft"))),
          BsonField.Name("public enemy #1") ->
            -\/(ExprOp.DocField(BsonField.Name("rIght"))))),
          IgnoreId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        city   <- read.projectField("city").makeObject("city")
        dist   <- city.distinctBy(city)
      } yield dist.build

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))),
            IgnoreId),
          groupOp(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
            \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))))),
          projectOp(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
            ExcludeId)))
    }
  }
}
