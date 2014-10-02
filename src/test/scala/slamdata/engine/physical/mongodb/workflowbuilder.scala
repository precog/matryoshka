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
              AnonFunDecl(List("key"),
                List(Return(AnonElem(List(
                  Ident("key"),
                  Access(Select(Ident("this"), "value"), Num(1, false)))))))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> \/-(Reshape.Doc(ListMap(
                BsonField.Name("lEft") -> -\/(ExprOp.DocVar.ROOT())))))))),
          chain(
            readOp(Collection("zips")),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("enemies")))))),
            mapOp(
              AnonFunDecl(List("key"),
                List(Return(AnonElem(List(
                  Ident("key"),
                  Access(Select(Ident("this"), "value"), Num(0, false)))))))),
            projectOp(Reshape.Doc(ListMap(
              BsonField.Name("rIght") -> -\/(ExprOp.DocVar.ROOT())))),
            reduceOp(
              AnonFunDecl(List("key", "values"),
                List(
                  VarDef(List("rez" -> AnonObjDecl(Nil))),
                  Call(Select(Ident("values"), "forEach"),
                    List(
                      AnonFunDecl(List("value"),
                        List(
                          ForIn(Ident("attr"),Ident("value"),
                            If(Call(Select(Ident("value"), "hasOwnProperty"), List(Ident("attr"))),
                              BinOp("=", Access(Ident("rez"), Ident("attr")), Access(Ident("value"),Ident("attr"))),
                              None)))))),
                  Return(Ident("rez"))))))),
        projectOp(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("lEft") \ BsonField.Name("value"))),
          BsonField.Name("public enemy #1") ->
            -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("rIght") \ BsonField.Name("value"))))))))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = for {
        city   <- read.projectField("city").makeObject("city")
        dist   <- city.distinct
      } yield dist.normalize

      op must beRightDisjOrDiff(chain(
          readOp(Collection("zips")),
          groupOp(
            Grouped(ListMap(
              BsonField.Name("city") -> ExprOp.First(ExprOp.DocVar.ROOT(BsonField.Name("city"))))),
            \/- (Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city")))))))))
    }
  }
}
