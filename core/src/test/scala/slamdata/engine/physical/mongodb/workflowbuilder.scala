package slamdata.engine.physical.mongodb

import org.specs2.mutable._
import org.specs2.execute.{Result}

import scala.collection.immutable.ListMap

import scalaz._, Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, TreeMatchers}
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
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/ (DocVar.ROOT(BsonField.Name("city"))))),
          IgnoreId)))
    }

    "make nested expression in single step" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        state <- lift(projectField(read, "state"))
        x1    <- expr2(city, pure(Bson.Text(", ")))(Concat(_, _, Nil))
        x2    <- expr2(x1, state)(Concat(_, _, Nil))
        zero =  makeObject(x2, "0")
      } yield zero).evalZero

      op must beRightDisjOrDiff(
        DocBuilder(
          WorkflowBuilder.read(Collection("zips")),
          ListMap(BsonField.Name("0") -> -\/(Concat(Concat(DocField(BsonField.Name("city")), Literal(Bson.Text(", ")), Nil), DocField(BsonField.Name("state")), Nil)))))
    }

    "make nested expression under shape preserving in single step" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        pop   <- lift(projectField(read, "pop"))
        filtered = filter(read, List(pop), { case p :: Nil => Selector.Doc(p -> Selector.Lt(Bson.Int32(1000))) })
        city  <- lift(projectField(filtered, "city"))
        state <- lift(projectField(filtered, "state"))
        x1    <- expr2(city, pure(Bson.Text(", ")))(Concat(_, _, Nil))
        x2    <- expr2(x1, state)(Concat(_, _, Nil))
        zero =  makeObject(x2, "0")
      } yield zero).evalZero

      op must beRightDisjOrDiff(
        ShapePreservingBuilder(
          DocBuilder(
            WorkflowBuilder.read(Collection("zips")),
            ListMap(BsonField.Name("0") -> -\/(Concat(Concat(DocField(BsonField.Name("city")), Literal(Bson.Text(", ")), Nil), DocField(BsonField.Name("state")), Nil)))),
          List(ExprBuilder(
            WorkflowBuilder.read(Collection("zips")),
            -\/(DocField(BsonField.Name("pop"))))),
          { case f :: Nil => $match(Selector.Doc(f -> Selector.Lt(Bson.Int32(1000)))) }))
    }

    "combine array with constant value" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val pureArr = pure(Bson.Arr(List(Bson.Int32(0), Bson.Int32(1))))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        array  <- arrayConcat(makeArray(city), pureArr)
        state2 <- projectIndex(array, 2)
      } yield state2).evalZero

      op must_== expr1(read)(κ(Literal(Bson.Int32(1))))
    }.pendingUntilFixed("#610")

    "elide array with known projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        state  <- lift(projectField(read, "state"))
        array  <- arrayConcat(makeArray(city), makeArray(state))
        state2 <- projectIndex(array, 1)
      } yield state2).evalZero

      op must_== (projectField(read, "state"))
    }

    "error with out-of-bounds projection" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        state  <- lift(projectField(read, "state"))
        array  <- arrayConcat(makeArray(city), makeArray(state))
        state2 <- projectIndex(array, 2)
      } yield state2).evalZero

      op must beLeftDisj(WorkflowBuilderError.InvalidOperation(
        "projectIndex", "array does not contain index ‘2’."))
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
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/ (DocVar.ROOT(BsonField.Name("city"))),
            BsonField.Name("pop") -> -\/ (DocVar.ROOT(BsonField.Name("pop"))))),
            IgnoreId)))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        key  <- lift(projectField(read, "city"))
        sort =  sortBy(read, List(key), Ascending :: Nil)
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
          "long" ->
            Access(Select(value, "loc").fix,
              Literal(Js.Num(1, false)).fix).fix,
          "public enemy #1" ->
            Access(Select(value, "enemies").fix,
              Literal(Js.Num(0, false)).fix).fix)).fix), Nil)))
    }

    "group on multiple fields" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        state   <- lift(projectField(read, "state"))
        grouped =  groupBy(read, List(city, state))
        sum     <- lift(projectField(grouped, "pop")).map(reduce(_)(Sum(_)))
        rez     <- build(sum)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__tmp0") -> Sum(DocField(BsonField.Name("pop"))))),
          \/-(Reshape(ListMap(
            BsonField.Name("0") -> -\/(DocField(BsonField.Name("city"))),
            BsonField.Name("1") -> -\/(DocField(BsonField.Name("state"))))))),
        $project(
          Reshape(ListMap(
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
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> First(DocVar.ROOT()))),
            \/-(Reshape(ListMap(
              BsonField.Name("city") -> -\/(DocVar.ROOT(BsonField.Name("city"))))))),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))))),
            ExcludeId)))
    }

    "distinct after group" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        city1   <- lift(projectField(read, "city"))
        grouped =  groupBy(read, List(city1))
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
          \/-(Reshape(ListMap(
            BsonField.Name("total") -> -\/(DocField(BsonField.Name("total"))),
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))))))),
        $project(Reshape(ListMap(
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
        sorted =  sortBy(projs, List(city, state), List(Ascending, Ascending))

        // NB: the compiler would not generate this op between sort and distinct
        lim    =  limit(sorted, 10)

        dist   <- distinctBy(lim, List(lim))
        rez    <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("zips")),
        $sort(NonEmptyList(
          BsonField.Name("city") -> Ascending,
          BsonField.Name("state") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))),
          BsonField.Name("state") -> -\/(DocField(BsonField.Name("state"))))),
          IgnoreId),
        $limit(10),
        $group(
          Grouped(ListMap(
            BsonField.Name("__tmp0") -> First(DocVar.ROOT()),
            BsonField.Name("__sd_key_0") ->
              First(DocField(BsonField.Name("city"))),
            BsonField.Name("__sd_key_1") ->
              First(DocField(BsonField.Name("state"))))),
          \/-(Reshape(ListMap(
            BsonField.Name("city") -> -\/(DocField(BsonField.Name("city"))),
            BsonField.Name("state") ->
              -\/(DocField(BsonField.Name("state"))))))),
        $sort(NonEmptyList(
          BsonField.Name("__sd_key_0") -> Ascending,
          BsonField.Name("__sd_key_1") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("city") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("city"))),
          BsonField.Name("state") ->
            -\/(DocField(BsonField.Name("__tmp0") \ BsonField.Name("state"))))),
          ExcludeId)))
    }

    "group in proj" in {
      val read = WorkflowBuilder.read(Collection("zips"))
      val op = (for {
        pop     <- lift(projectField(read, "pop"))
        grouped =  groupBy(pop, List(pure(Bson.Int32(1))))
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
      val one  = expr1(read)(κ(Literal(Bson.Int32(1))))
      val obj  = makeObject(reduce(groupBy(one, List(one)))(Sum(_)), "total")
      val op   = build(obj).evalZero

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
      val cp   = makeObject(
        reduce(expr1(read)(κ(Literal(Bson.Int32(1)))))(Sum(_)),
        "count")
      val op = (for {
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
        grouped =  groupBy(pop, List(city))
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
        grouped =  groupBy(read, List(city))
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
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> Sum(DocField(BsonField.Name("pop"))),
              BsonField.Name("city") -> Push(DocField(BsonField.Name("city"))))),
            -\/ (DocField(BsonField.Name("city")))),
          $unwind(DocField(BsonField.Name("city")))))
    }

    "group in expression" in {
      val read    = WorkflowBuilder.read(Collection("zips"))
      val grouped = groupBy(read, List(pure(Bson.Int32(1))))
      val op = (for {
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
            $project(Reshape(ListMap(
              BsonField.Name("totalInK") ->
                -\/(Divide(
                  DocField(BsonField.Name("__tmp2")),
                  Literal(Bson.Int32(1000)))))),
          IgnoreId)))
    }
  }

  "RenderTree[WorkflowBuilder]" should {
    def render(op: WorkflowBuilder)(implicit RO: RenderTree[WorkflowBuilder]):
        String =
      RO.render(op).draw.mkString("\n")

    val read = WorkflowBuilder.read(Collection("zips"))

    "render in-process group" in {
      val grouped = groupBy(read, List(pure(Bson.Int32(1))))
      val op = for {
        pop <- projectField(grouped, "pop")
      } yield reduce(pop)(Sum(_))
      op.map(render) must beRightDisj(
        """GroupBuilder
          |├─ ExprBuilder
          |│  ├─ CollectionBuilder
          |│  │  ├─ $Read(zips)
          |│  │  ├─ ExprOp(DocVar.ROOT())
          |│  │  ╰─ Schema(None)
          |│  ╰─ ExprOp(DocField(BsonField.Name("pop")))
          |├─ By
          |│  ╰─ ValueBuilder(Int32(1))
          |├─ Content
          |│  ╰─ \/-
          |│     ╰─ GroupOp(Sum(DocVar.ROOT()))
          |╰─ Id(e50d6965)""".stripMargin)
    }

  }
}
