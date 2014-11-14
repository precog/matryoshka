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

      op must_== $read(Collection("zips"))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        city2 =  makeObject(city, "city")
        rez   <- emitSt(build(city2))
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("city") -> -\/ (DocVar.ROOT(BsonField.Name("city"))))),
          IgnoreId)))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        pop    <- lift(projectField(read, "pop"))
        left   =  makeObject(city, "city")
        right  =  makeObject(pop, "pop")
        merged <- objectConcat(left, right)
        rez    <- emitSt(build(merged))
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
        arr  <- emitSt(makeArray(key))
        sort <- sortBy(read, arr, Ascending :: Nil)
        rez  <- emitSt(build(sort))
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("__tmp1") -> \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (DocField(BsonField.Name("city")))))),
            BsonField.Name("__tmp2") -> -\/ (DocVar.ROOT()))),
            ExcludeId),
          $sort(
            NonEmptyList(
              BsonField.Name("__tmp1") \ BsonField.Index(0) -> Ascending)),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("value") -> -\/ (DocField(BsonField.Name("__tmp2"))))),
            ExcludeId)))
    }

    "merge unmergables" in {
      import Js._

      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        l    <- lift(projectField(read, "loc")).flatMap(projectIndex(_, 1))
        r    <- lift(projectField(read, "enemies")).flatMap(projectIndex(_, 0))
        lobj =  makeObject(l, "long")
        robj =  makeObject(r, "public enemy #1")
        merged <- objectConcat(lobj, robj)
        rez    <- emitSt(build(merged))
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $foldLeft(
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp0") -> -\/(DocField(BsonField.Name("loc"))))),
              IgnoreId),
            $map(
              $Map.mapMap("value",
                Access(Access(Ident("value"), Str("__tmp0")), Num(1, false)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp3") -> -\/(DocVar.ROOT()))),
              IncludeId)),
          chain(
            $read(Collection("zips")),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp1") -> -\/(DocField(BsonField.Name("enemies"))))),
              IgnoreId),
            $map(
              $Map.mapMap("value",
                Access(Access(Ident("value"), Str("__tmp1")), Num(0, false)))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("__tmp4") -> -\/(DocVar.ROOT()))),
              IncludeId))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("long") ->
            -\/(DocField(BsonField.Name("__tmp3"))),
          BsonField.Name("public enemy #1") ->
            -\/(DocField(BsonField.Name("__tmp4"))))),
          IgnoreId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        proj <- lift(projectField(read, "city"))
        city =  makeObject(proj, "city")
        dist <- distinctBy(city, city)
        rez  <- emitSt(build(dist))
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("zips")),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/ (DocField(BsonField.Name("city"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("value") -> First(DocVar.ROOT()))),
            \/- (Reshape.Arr(ListMap(
              BsonField.Index(0) -> -\/ (DocVar.ROOT(BsonField.Name("city"))))))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
            ExcludeId)))
    }
    
    "distinct after group" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city1   <- lift(projectField(read, "city"))
        arr     <- emitSt(makeArray(city1))
        grouped <- emitSt(groupBy(read, arr))
        total   =  reduce(grouped)(Sum(_))
        proj0   =  makeObject(total, "total")
        city2   <- lift(projectField(grouped, "city"))
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0,  proj1)
        dist    <- distinctBy(projs, projs)
        rez     <- emitSt(build(dist))
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("__tmp1") -> \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(DocField(BsonField.Name("city")))))),
          BsonField.Name("__tmp2") -> -\/(DocVar.ROOT()))),
          ExcludeId),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> Sum(DocField(BsonField.Name("__tmp2"))),
            BsonField.Name("city") -> Push(DocField(BsonField.Name("__tmp2") \ BsonField.Name("city"))))),
          -\/(DocField(BsonField.Name("__tmp1")))),
        $unwind(
          DocField(BsonField.Name("city"))),
        $group(
          Grouped(ListMap(
            BsonField.Name("value") -> First(DocVar.ROOT()))),
          \/-(Reshape.Arr(ListMap(
            BsonField.Index(0) -> -\/(DocField(BsonField.Name("total"))),
            BsonField.Index(1) -> -\/(DocField(BsonField.Name("city"))))))),
        $project(Reshape.Doc(ListMap(
          BsonField.Name("total") -> -\/ (DocField(BsonField.Name("value") \ BsonField.Name("total"))),
          BsonField.Name("city") -> -\/ (DocField(BsonField.Name("value") \ BsonField.Name("city"))))),
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
        larr   <- emitSt(makeArray(key0))
        rarr   <- emitSt(makeArray(key1))
        keys   <- arrayConcat(larr, rarr)
        sorted <- sortBy(projs, keys, List(Ascending, Ascending))

        // NB: the compiler would not generate this op between sort and distinct
        lim    <- emitSt(appendOp(sorted, $limit(10)))

        dist   <- distinctBy(lim, lim)
        rez    <- emitSt(build(dist))
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
        grouped <- emitSt(groupBy(pop, pure(Bson.Int32(1))))
        total   =  reduce(grouped)(Sum(_))
        obj     =  makeObject(total, "total")
        rez     <- emitSt(build(obj))
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
      val one  = expr1(read)(_ => Literal(Bson.Int32(1)))
      val op = (for {
        grouped <- groupBy(one, one)
        total   <- state(reduce(grouped)(Sum(_)))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero
  
      op must beTree(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(Literal(Bson.Int32(1))))),
            -\/(Literal(Bson.Null))
          )))
    }
  
    "group in two projs" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val one  = expr1(read)(_ => Literal(Bson.Int32(1)))
      val op = (for {
        grouped1 <- emitSt(groupBy(one, one))
        count    =  reduce(grouped1)(Sum(_))
        cp       =  makeObject(count, "count")

        pop      <- lift(projectField(read, "pop"))
        grouped2 <- emitSt(groupBy(pop, one))
        total    =  reduce(grouped2)(Sum(_))
        tp       =  makeObject(total, "total")
      
        proj     <- objectConcat(cp, tp)
        rez      <- emitSt(build(proj))
      } yield rez).evalZero
    
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("__sd_tmp_1") -> Sum(Literal(Bson.Int32(1))),
              BsonField.Name("__sd_tmp_2") -> Sum(DocField(BsonField.Name("pop"))))),
            -\/(Literal(Bson.Null))),
          $project(Reshape.Doc(ListMap(
            BsonField.Name("count") -> -\/ (DocField(BsonField.Name("__sd_tmp_1"))),
            BsonField.Name("total") -> -\/ (DocField(BsonField.Name("__sd_tmp_2"))))),
            IncludeId)))
    }

    "group on a field" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        pop     <- lift(projectField(read, "pop"))
        grouped <- emitSt(groupBy(pop, city))
        total   =  reduce(grouped)(Sum(_))
        obj     =  makeObject(total, "total")
        rez     <- emitSt(build(obj))
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
        grouped <- emitSt(groupBy(read, city))
        city2   <- lift(projectField(grouped, "city"))
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)(Sum(_))
        proj0   =  makeObject(total, "total")
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0, proj1)
        rez     <- emitSt(build(projs))
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
        grouped <- emitSt(groupBy(read, pure(Bson.Int32(1))))
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)(Sum(_))
        expr    <- emitSt(expr2(total, pure(Bson.Int32(1000)))(Divide(_, _)))
        inK     =  makeObject(expr, "totalInK")
        rez     <- emitSt(build(inK))
      } yield rez).evalZero
  
      op must beRightDisjOrDiff(
        chain($read(Collection("zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") ->
                Sum(DocField(BsonField.Name("pop"))))),
            -\/(Literal(Bson.Null))),
            $project(Reshape.Doc(ListMap(
              BsonField.Name("totalInK") -> -\/ (Divide(
                DocField(BsonField.Name("__tmp0")),
                Literal(Bson.Int32(1000)))))),
          IncludeId)))
    }
  } 
}
