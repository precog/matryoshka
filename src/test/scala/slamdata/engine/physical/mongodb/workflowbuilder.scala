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
  import Reshape._
  import Workflow._
  import WorkflowBuilder._
  import IdHandling._

  val readZips = WorkflowBuilder.read(Collection("zips"))
  def pureInt(n: Int) = WorkflowBuilder.pure(Bson.Int32(n))

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = WorkflowBuilder.read(Collection("zips")).build

      op must_== $read(Collection("zips"))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city = read.projectField("city").makeObject("city")
      val op = city.build

      op must_== 
        chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))),
            IgnoreId))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val left = read.projectField("city").makeObject("city")
      val right = read.projectField("pop").makeObject("pop")
      val op = (for {
        merged <- left objectConcat right
      } yield merged.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("pop"))))),
            IgnoreId)))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val keys = read.projectField("city").makeArray
      val op = (for {
        sort <- read.sortBy(keys, Ascending :: Nil)
      } yield sort.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("__tmp0") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
            BsonField.Name("__tmp1") -> -\/ (ExprOp.DocVar.ROOT()))),
            ExcludeId),
          $sort(
            NonEmptyList(
              BsonField.Name("__tmp0") \ BsonField.Index(0) -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/ (ExprOp.DocField(BsonField.Name("__tmp1"))))),
            ExcludeId)))
    }

    "merge unmergables" in {
      import Js._

      val read = WorkflowBuilder.read(Collection("zips"))
      val left = read.projectField("loc").projectIndex(1).makeObject("long")
      val right = read.projectField("enemies").projectIndex(0).makeObject("public enemy #1")
      val op = (for {
        merged <- left objectConcat right
      } yield merged.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
        $foldLeft(
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("loc"))))),
              IgnoreId),
            $map(
              $Map.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(1, false)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId)),
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("value") -> -\/(ExprOp.DocField(BsonField.Name("enemies"))))),
              IgnoreId),
            $map(
              $Map.mapMap("value",
                Access(Access(Ident("value"), Str("value")), Num(0, false)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp1") -> -\/(ExprOp.DocVar.ROOT()))),
              IncludeId))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(ExprOp.DocField(BsonField.Name("__tmp0"))),
          BsonField.Name("public enemy #1") ->
            -\/(ExprOp.DocField(BsonField.Name("__tmp1"))))),
          IgnoreId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city = read.projectField("city").makeObject("city")
      val op = (for {
        dist   <- city.distinctBy(city)
      } yield dist.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
            \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (ExprOp.DocVar.ROOT(BsonField.Name("city"))))))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
            ExcludeId)))
    }
    
    "distinct after group" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city1 = read.projectField("city")
      val op = (for {
        grouped <- read.groupBy(city1.makeArray)
        
        pop     = grouped.projectField("pop")
        total   <- grouped.reduce(ExprOp.Sum(_))
        city2    = grouped.projectField("city")
        proj0   = total.makeObject("total")
        proj1   = city2.makeObject("city")
        projs   <- proj0 objectConcat proj1
        
        dist    <- projs.distinctBy(projs)
      } yield dist.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("__tmp1") -> \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(ExprOp.DocField(BsonField.Name("city")))))),
          BsonField.Name("__tmp2") -> -\/(ExprOp.DocVar.ROOT()))),
          ExcludeId),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("__tmp2"))),
            BsonField.Name("city") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("__tmp2") \ BsonField.Name("city"))))),
          -\/(ExprOp.DocField(BsonField.Name("__tmp1")))),
        $unwind(
          ExprOp.DocField(BsonField.Name("city"))),
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> ExprOp.First(ExprOp.DocVar.ROOT()))),
          \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(ExprOp.DocField(BsonField.Name("total"))),
            BsonField.Index(1) -> -\/(ExprOp.DocField(BsonField.Name("city"))))))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("total") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("total"))),
          BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
          ExcludeId)))
    }

    "distinct and sort with intervening op" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city = read.projectField("city").makeObject("city")
      val state = read.projectField("state").makeObject("state")
      val op = (for {
        projs  <- city objectConcat state
      
        key0   =  projs.projectField("city").makeObject("key")
        key1   =  projs.projectField("state").makeObject("key")
        keys   <- key0.makeArray arrayConcat key1.makeArray
        sorted <- projs.sortBy(keys, List(Ascending, Ascending))

        lim    = sorted >>> $limit(10)  // Note: the compiler would not generate this op between sort and distinct

        dist   <- lim.distinctBy(lim)
      } yield dist.build).runZero.map(_._2)

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (ExprOp.DocField(BsonField.Name("state")))))),
            BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
              BsonField.Index(1) -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("key") -> -\/ (ExprOp.DocField(BsonField.Name("state"))))))))))),
            IncludeId),
          $sort(NonEmptyList(
            BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending,
            BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key") -> Ascending)),
          $limit(10),
          $group(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.First(ExprOp.DocField(BsonField.Name("lEft"))),
              BsonField.Name("__sd_key_0") -> ExprOp.First(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key"))),
              BsonField.Name("__sd_key_1") -> ExprOp.First(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key"))))),
            -\/ (ExprOp.DocVar.ROOT(BsonField.Name("lEft")))),
          $sort(NonEmptyList(
            BsonField.Name("__sd_key_0") -> Ascending,
            BsonField.Name("__sd_key_1") -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("city"))),
            BsonField.Name("state") -> -\/(ExprOp.DocField(BsonField.Name("value") \ BsonField.Name("state"))))),
            IncludeId)))
    }.pendingUntilFixed("#378, but there are more interesting cases")

    "group in proj" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val pop   = read.projectField("pop")
      val op = (for {
        grouped <- pop.groupBy(WorkflowBuilder.pure(Bson.Int32(1)))
        total   <- grouped.reduce(ExprOp.Sum(_))
        proj    =  total.makeObject("total")
      } yield proj.build).runZero.map(_._2)
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))))),
            -\/ (ExprOp.Literal(Bson.Int32(1)))
          )))
    }
  
    "group constant in proj" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        one     <- read.expr1(_ => \/- (ExprOp.Literal(Bson.Int32(1))))
        grouped <- one.groupBy(one)
        total   <- grouped.reduce(ExprOp.Sum(_))
        proj    =  total.makeObject("total")
      } yield proj.build).runZero.map(_._2)
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))))),
            -\/ (ExprOp.Literal(Bson.Int32(1)))
          )))
    }
  
    "group in two projs" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val pop   = read.projectField("pop")
      val op = (for {
        one      <- read.expr1(_ => \/- (ExprOp.Literal(Bson.Int32(1))))
        grouped1 <- one.groupBy(one)
        count    <- grouped1.reduce(ExprOp.Sum(_))
        cp       =  count.makeObject("count")

        grouped2 <- pop.groupBy(one)
        total    <- grouped2.reduce(ExprOp.Sum(_))
        tp       =  total.makeObject("total")
      
        proj     <- cp objectConcat tp
      } yield proj.build).runZero.map(_._2)
    
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("__sd_tmp_1") -> ExprOp.Sum(ExprOp.Literal(Bson.Int32(1))),
              BsonField.Name("__sd_tmp_2") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))))),
            -\/ (ExprOp.Literal(Bson.Int32(1)))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("count") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1"))),
            BsonField.Name("total") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_2"))))),
            IncludeId)))
    }

    "group on a field" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city = read.projectField("city")
      val pop  = read.projectField("pop")
      val op = (for {
        grouped <- pop.groupBy(city)
        total <- grouped.reduce(ExprOp.Sum(_))
        proj  = total.makeObject("total")
      } yield proj.build).runZero.map(_._2)

      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))))),
            -\/ (ExprOp.DocField(BsonField.Name("city"))))))
    }

    "group on a field, with un-grouped projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val city = read.projectField("city")
      val pop  = read
      val op = (for {
        grouped <- read.groupBy(city)
        
        total   <- grouped.projectField("pop").reduce(ExprOp.Sum(_))
        proj0   = total.makeObject("total")
        proj1   = grouped.projectField("city").makeObject("city")
        projs   <- proj0 objectConcat proj1
      } yield projs.build).runZero.map(_._2)

      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("city")))))),
            BsonField.Name("rIght") -> -\/ (ExprOp.DocVar.ROOT()))),
            IncludeId),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("pop"))),
              BsonField.Name("__sd_tmp_1") -> ExprOp.Push(ExprOp.DocField(BsonField.Name("lEft"))))),
            -\/ (ExprOp.DocField(BsonField.Name("rIght") \ BsonField.Name("city")))),
          $unwind(
            ExprOp.DocField(BsonField.Name("__sd_tmp_1"))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("total") -> -\/ (ExprOp.DocField(BsonField.Name("total"))),
            BsonField.Name("city") -> -\/ (ExprOp.DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))))),
            IncludeId)))
    }

    "group in expression" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        grouped <- read.groupBy(WorkflowBuilder.pure(Bson.Int32(1)))
        total   <- grouped.projectField("pop").reduce(ExprOp.Sum(_))
        expr    <- total.expr2(WorkflowBuilder.pure(Bson.Int32(1000)))((l, r) => \/- (ExprOp.Divide(l, r)))
        proj    = expr.makeObject("totalInK")
      } yield proj.build).runZero.map(_._2)
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("value") -> ExprOp.Sum(ExprOp.DocField(BsonField.Name("pop"))))),
            -\/ (ExprOp.Literal(Bson.Int32(1)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("totalInK") -> -\/ (ExprOp.Divide(
                ExprOp.DocField(BsonField.Name("value")),
                ExprOp.Literal(Bson.Int32(1000)))))),
          IncludeId)))
    }
  } 
}
