package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.execute.{Result}

import scala.collection.immutable.ListMap

import scalaz._, Scalaz._

import slamdata.engine.fp._
import slamdata.engine.javascript._

import slamdata.engine.{DisjunctionMatchers, TreeMatchers}
import slamdata.specs2._

class WorkflowBuilderSpec
    extends Specification
    with DisjunctionMatchers
    with TreeMatchers
    with PendingWithAccurateCoverage {
  import Reshape._
  import Workflow._
  import WorkflowBuilder._
  import IdHandling._
  import ExprOp._

  val readZips = WorkflowBuilder.read(Collection("zips"))
  def pureInt(n: Int) = WorkflowBuilder.pure(Bson.Int32(n))

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = build(read(Collection("zips"))).evalZero

      op must beRightDisj($read(Collection("zips")))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        city2 =  makeObject(city, "city")
        rez   <- build(city2)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/ (DocVar.ROOT(BsonField.Name("city"))))),
          IgnoreId)))
    }

    "project field from value" in {
      val value = pure(Bson.Doc(ListMap(
        "foo" -> Bson.Int32(1),
        "bar" -> Bson.Int32(2))))
      projectField(value, "bar") must
        beRightDisjOrDiff(pure(Bson.Int32(2)))
    }

    "project index from value" in {
      val value = pure(Bson.Arr(List(Bson.Int32(1), Bson.Int32(2))))
      projectIndex(value, 1).evalZero must
        beRightDisjOrDiff(pure(Bson.Int32(2)))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        pop    <- lift(projectField(read, "pop"))
        left   =  makeObject(city, "city")
        right  =  makeObject(pop, "pop")
        merged <- objectConcat(left, right)
        rez    <- build(merged)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (DocVar.ROOT(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/ (DocVar.ROOT(BsonField.Name("pop"))))),
            IgnoreId)))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        key  <- lift(projectField(read, "city"))
        sort <- sortBy(read, List(key), Ascending :: Nil)
        rez  <- build(sort)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $sort(NonEmptyList(BsonField.Name("city") -> Ascending))))
    }

    "merge index projections" in {
      import JsCore._

      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        l    <- lift(projectField(read, "loc")).flatMap(projectIndex(_, 1))
        r    <- lift(projectField(read, "enemies")).flatMap(projectIndex(_, 0))
        lobj =  makeObject(l, "long")
        robj =  makeObject(r, "public enemy #1")
        merged <- objectConcat(lobj, robj)
        rez    <- build(merged)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $simpleMap(JsMacro(value => Obj(ListMap(
          "__tmp0" ->
            Access(Select(value, "loc").fix,
              Literal(Js.Num(1, false)).fix).fix,
          "__tmp1" ->
            Access(Select(value, "enemies").fix,
              Literal(Js.Num(0, false)).fix).fix)).fix)),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(DocField(BsonField.Name("__tmp0"))),
          BsonField.Name("public enemy #1") ->
            -\/(DocField(BsonField.Name("__tmp1"))))),
          IgnoreId)))
    }

    "group on multiple fields" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        state   <- lift(projectField(read, "state"))
        grouped <- groupBy(read, List(city, state))
        sum     <- lift(projectField(grouped, "pop")).map(reduce(_)(Sum(_)))
        rez     <- build(sum)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__tmp0") -> Sum(DocField(BsonField.Name("pop"))))),
          \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(DocField(BsonField.Name("city"))),
            BsonField.Index(1) -> -\/(DocField(BsonField.Name("state"))))))),
        $project(
          Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/(DocField(BsonField.Name("__tmp0"))))),
            ExcludeId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        proj <- lift(projectField(read, "city"))
        city =  makeObject(proj, "city")
        dist <- distinctBy(city, List(city))
        rez  <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> First(DocVar.ROOT()))),
            \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (DocVar.ROOT(BsonField.Name("city"))))))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
            ExcludeId)))
    }
    
    "distinct after group" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city1   <- lift(projectField(read, "city"))
        grouped <- groupBy(read, List(city1))
        total   =  reduce(grouped)(Sum(_))
        proj0   =  makeObject(total, "total")
        city2   <- lift(projectField(grouped, "city"))
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0,  proj1)
        dist    <- distinctBy(projs, List(projs))
        rez     <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> Sum(DocVar.ROOT()),
            BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))))),
          -\/(DocField(BsonField.Name("city")))),
        $unwind(DocField(BsonField.Name("city"))),
        $group(
          Grouped(ListMap(BsonField.Name("__tmp0") -> First(DocVar.ROOT()))),
          \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(DocField(BsonField.Name("total"))),
            BsonField.Index(1) -> -\/(DocField(BsonField.Name("city"))))))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("total") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("total"))),
          BsonField.Name("city") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
          ExcludeId)))
    }

    "distinct and sort with intervening op" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        state  <- lift(projectField(read, "state"))
        left   =  makeObject(city, "city")
        right  =  makeObject(state, "state")
        projs  <- objectConcat(left, right)
        city2  <- lift(projectField(projs, "city"))
        state2 <- lift(projectField(projs, "state"))
        key0   =  makeObject(city2, "key")
        key1   =  makeObject(state2, "key")
        sorted <- sortBy(projs, List(key0, key1), List(Ascending, Ascending))

        // NB: the compiler would not generate this op between sort and distinct
        lim    <- limit(sorted, 10)

        dist   <- distinctBy(lim, List(lim))
        rez    <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))),
              BsonField.Name("state") -> -\/ (DocField(BsonField.Name("state")))))),
            BsonField.Name("rIght") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("key") -> -\/ (DocField(BsonField.Name("city")))))),
              BsonField.Index(1) -> \/- (Reshape.Doc(ListMap(
                BsonField.Name("key") -> -\/ (DocField(BsonField.Name("state"))))))))))),
            IncludeId),
          $sort(NonEmptyList(
            BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key") -> Ascending,
            BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key") -> Ascending)),
          $limit(10),
          $group(
            Grouped(ListMap(
              BsonField.Name("value") -> First(DocField(BsonField.Name("lEft"))),
              BsonField.Name("__sd_key_0") -> First(DocField(BsonField.Name("rIght") \ BsonField.Index(0) \ BsonField.Name("key"))),
              BsonField.Name("__sd_key_1") -> First(DocField(BsonField.Name("rIght") \ BsonField.Index(1) \ BsonField.Name("key"))))),
            -\/ (DocVar.ROOT(BsonField.Name("lEft")))),
          $sort(NonEmptyList(
            BsonField.Name("__sd_key_0") -> Ascending,
            BsonField.Name("__sd_key_1") -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("value") \ BsonField.Name("city"))),
            BsonField.Name("state") -> -\/(DocField(BsonField.Name("value") \ BsonField.Name("state"))))),
            IncludeId)))
    }.pendingUntilFixed("#378, but there are more interesting cases")

    "group in proj" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        pop     <- lift(projectField(read, "pop"))
        grouped <- groupBy(pop, List(pure(Bson.Int32(1))))
        total   =  reduce(grouped)(Sum(_))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(DocField(BsonField.Name("pop"))))),
            -\/ (Literal(Bson.Null)))))
    }

    "group constant in proj" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        one     <- expr1(read)(_ => Literal(Bson.Int32(1)))
        grouped <- groupBy(one, List(one))
        total   =  reduce(grouped)(Sum(_))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(Literal(Bson.Int32(1))))),
            -\/(Literal(Bson.Null))
          )))
    }
  
    "group in two projs" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        one      <- expr1(read)(_ => Literal(Bson.Int32(1)))
        count    =  reduce(one)(Sum(_))
        cp       =  makeObject(count, "count")

        pop      <- lift(projectField(read, "pop"))
        total    =  reduce(pop)(Sum(_))
        tp       =  makeObject(total, "total")
      
        proj     <- objectConcat(cp, tp)
        rez      <- build(proj)
      } yield rez).evalZero
    
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("count") -> Sum(Literal(Bson.Int32(1))),
              BsonField.Name("total") -> Sum(DocField(BsonField.Name("pop"))))),
            -\/(Literal(Bson.Null)))))
    }

    "group on a field" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        pop     <- lift(projectField(read, "pop"))
        grouped <- groupBy(pop, List(city))
        total   =  reduce(grouped)(Sum(_))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(DocField(BsonField.Name("pop"))))),
            -\/ (DocField(BsonField.Name("city"))))))
    }

    "group on a field, with un-grouped projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        grouped <- groupBy(read, List(city))
        city2   <- lift(projectField(grouped, "city"))
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)(Sum(_))
        proj0   =  makeObject(total, "total")
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0, proj1)
        rez     <- build(projs)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("lEft") -> \/- (Reshape.Doc(ListMap(
              BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city")))))),
            BsonField.Name("rIght") -> -\/ (DocVar.ROOT()))),
            IncludeId),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(DocField(BsonField.Name("rIght") \ BsonField.Name("pop"))),
              BsonField.Name("__sd_tmp_1") -> Push(DocField(BsonField.Name("lEft"))))),
            -\/ (DocField(BsonField.Name("rIght") \ BsonField.Name("city")))),
          $unwind(
            DocField(BsonField.Name("__sd_tmp_1"))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("total") -> -\/ (DocField(BsonField.Name("total"))),
            BsonField.Name("city") -> -\/ (DocField(BsonField.Name("__sd_tmp_1") \ BsonField.Name("city"))))),
            IncludeId)))
    }

    "group in expression" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        grouped <- groupBy(read, List(pure(Bson.Int32(1))))
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)(Sum(_))
        expr    <- expr2(total, pure(Bson.Int32(1000)))(Divide(_, _))
        inK     =  makeObject(expr, "totalInK")
        rez     <- build(inK)
      } yield rez).evalZero
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp2") ->
                Sum(DocField(BsonField.Name("pop"))))),
            -\/(Literal(Bson.Null))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("totalInK") ->
                -\/(Divide(
                  DocField(BsonField.Name("__tmp2")),
                  Literal(Bson.Int32(1000)))))),
          IgnoreId)))
    }
  } 
}
