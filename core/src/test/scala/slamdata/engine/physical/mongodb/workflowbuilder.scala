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
  import Workflow._
  import WorkflowBuilder._
  import IdHandling._
  import slamdata.engine.physical.mongodb.accumulator._
  import slamdata.engine.physical.mongodb.expression._

  val readZips = WorkflowBuilder.read(Collection("db", "zips"))
  def pureInt(n: Int) = WorkflowBuilder.pure(Bson.Int32(n))

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = build(read(Collection("db", "zips"))).evalZero

      op must beRightDisj($read(Collection("db", "zips")))
    }

    "make simple projection" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        city2 =  makeObject(city, "city")
        rez   <- build(city2)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/($var("city")))),
          IgnoreId)))
    }

    "make nested expression in single step" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        state <- lift(projectField(read, "state"))
        x1    <- expr2(city, pure(Bson.Text(", ")))($concat(_, _))
        x2    <- expr2(x1, state)($concat(_, _))
        zero =  makeObject(x2, "0")
      } yield zero).evalZero

      op must beRightDisjOrDiff(
        DocBuilder(
          WorkflowBuilder.read(Collection("db", "zips")),
          ListMap(BsonField.Name("0") ->
            -\/($concat($concat($var("city"), $literal(Bson.Text(", "))), $var("state"))))))
    }

    "make nested expression under shape preserving in single step" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        pop   <- lift(projectField(read, "pop"))
        filtered = filter(read, List(pop), { case p :: Nil => Selector.Doc(p -> Selector.Lt(Bson.Int32(1000))) })
        city  <- lift(projectField(filtered, "city"))
        state <- lift(projectField(filtered, "state"))
        x1    <- expr2(city, pure(Bson.Text(", ")))($concat(_, _))
        x2    <- expr2(x1, state)($concat(_, _))
        zero =  makeObject(x2, "0")
      } yield zero).evalZero

      op must beRightDisjOrDiff(
        ShapePreservingBuilder(
          DocBuilder(
            WorkflowBuilder.read(Collection("db", "zips")),
            ListMap(BsonField.Name("0") ->
              -\/($concat($concat($var("city"), $literal(Bson.Text(", "))), $var("state"))))),
          List(ExprBuilder(
            WorkflowBuilder.read(Collection("db", "zips")),
            -\/($var("pop")))),
          { case f :: Nil => $match(Selector.Doc(f -> Selector.Lt(Bson.Int32(1000)))) }))
    }

    "combine array with constant value" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val pureArr = pure(Bson.Arr(List(Bson.Int32(0), Bson.Int32(1))))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        array  <- arrayConcat(makeArray(city), pureArr)
        state2 <- lift(projectIndex(array, 2))
      } yield state2).evalZero

      op must_== expr1(read)(κ($literal(Bson.Int32(1))))
    }.pendingUntilFixed("#610")

    "elide array with known projection" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        state  <- lift(projectField(read, "state"))
        array  <- arrayConcat(makeArray(city), makeArray(state))
        state2 <- lift(projectIndex(array, 1))
      } yield state2).evalZero

      op must_== projectField(read, "state")
    }

    "error with out-of-bounds projection" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        state  <- lift(projectField(read, "state"))
        array  <- arrayConcat(makeArray(city), makeArray(state))
        state2 <- lift(projectIndex(array, 2))
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
      projectIndex(value, 1) must
        beRightDisjOrDiff(pure(Bson.Int32(2)))
    }

    "merge reads" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        pop    <- lift(projectField(read, "pop"))
        left   =  makeObject(city, "city")
        right  =  makeObject(pop, "pop")
        merged <- objectConcat(left, right)
        rez    <- build(merged)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("db", "zips")),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/($var("city")),
            BsonField.Name("pop")  -> -\/($var("pop")))),
            IgnoreId)))
    }

    "sorted" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        key  <- lift(projectField(read, "city"))
        sort =  sortBy(read, List(key), Ascending :: Nil)
        rez  <- build(sort)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $sort(NonEmptyList(BsonField.Name("city") -> Ascending))))
    }

    "merge index projections" in {
      import JsCore._

      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        l    <- lift(projectField(read, "loc").flatMap(projectIndex(_, 1)))
        r    <- lift(projectField(read, "enemies").flatMap(projectIndex(_, 0)))
        lobj =  makeObject(l, "long")
        robj =  makeObject(r, "public enemy #1")
        merged <- objectConcat(lobj, robj)
        rez    <- build(merged)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Ident("x"), Obj(ListMap(
          "long" ->
            Access(Select(Ident("x").fix, "loc").fix,
              Literal(Js.Num(1, false)).fix).fix,
          "public enemy #1" ->
            Access(Select(Ident("x").fix, "enemies").fix,
              Literal(Js.Num(0, false)).fix).fix)).fix))),
          ListMap())))
    }

    "group on multiple fields" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        state   <- lift(projectField(read, "state"))
        grouped =  groupBy(read, List(city, state))
        sum     <- lift(projectField(grouped, "pop")).map(reduce(_)($sum(_)))
        rez     <- build(sum)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("__tmp0") -> $sum($var("pop")))),
          \/-(Reshape(ListMap(
            BsonField.Name("0") -> -\/($var("city")),
            BsonField.Name("1") -> -\/($var("state")))))),
        $project(
          Reshape(ListMap(
            BsonField.Name("value") -> -\/($var("__tmp0")))),
            ExcludeId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        proj <- lift(projectField(read, "city"))
        city =  makeObject(proj, "city")
        dist <- distinctBy(city, List(city))
        rez  <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("db", "zips")),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/($var("city")))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp0") -> $first($$ROOT))),
            \/-(Reshape(ListMap(
              BsonField.Name("city") -> -\/($var("city")))))),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> -\/($var("__tmp0", "city")))),
            ExcludeId)))
    }

    "distinct after group" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city1   <- lift(projectField(read, "city"))
        grouped =  groupBy(read, List(city1))
        total   =  reduce(grouped)($sum(_))
        proj0   =  makeObject(total, "total")
        city2   <- lift(projectField(grouped, "city"))
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0,  proj1)
        dist    <- distinctBy(projs, List(projs))
        rez     <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> $sum($$ROOT),
            BsonField.Name("city")  -> $push($var("city")))),
          -\/($var("city"))),
        $unwind(DocField(BsonField.Name("city"))),
        $group(
          Grouped(ListMap(BsonField.Name("__tmp0") -> $first($$ROOT))),
          \/-(Reshape(ListMap(
            BsonField.Name("total") -> -\/($var("total")),
            BsonField.Name("city")  -> -\/($var("city")))))),
        $project(Reshape(ListMap(
          BsonField.Name("total") -> -\/($var("__tmp0", "total")),
          BsonField.Name("city")  -> -\/($var("__tmp0", "city")))),
          ExcludeId)))
    }

    "distinct and sort with intervening op" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
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
        $read(Collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> -\/($var("city")),
          BsonField.Name("state") -> -\/($var("state")))),
          IgnoreId),
        $sort(NonEmptyList(
          BsonField.Name("city") -> Ascending,
          BsonField.Name("state") -> Ascending)),
        $limit(10),
        $group(
          Grouped(ListMap(
            BsonField.Name("__tmp0")     -> $first($$ROOT),
            BsonField.Name("__sd_key_0") -> $first($var("city")),
            BsonField.Name("__sd_key_1") -> $first($var("state")))),
          \/-(Reshape(ListMap(
            BsonField.Name("city") -> -\/($var("city")),
            BsonField.Name("state") -> -\/($var("state")))))),
        $sort(NonEmptyList(
          BsonField.Name("__sd_key_0") -> Ascending,
          BsonField.Name("__sd_key_1") -> Ascending)),
        $project(Reshape(ListMap(
          BsonField.Name("city")  -> -\/($var("__tmp0", "city")),
          BsonField.Name("state") -> -\/($var("__tmp0", "state")))),
          ExcludeId)))
    }

    "group in proj" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        pop     <- lift(projectField(read, "pop"))
        grouped =  groupBy(pop, List(pure(Bson.Int32(1))))
        total   =  reduce(grouped)($sum(_))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> $sum($var("pop")))),
            -\/($literal(Bson.Null)))))
    }

    "group constant in proj" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        one <- expr1(read)(κ($literal(Bson.Int32(1))))
        obj =  makeObject(reduce(groupBy(one, List(one)))($sum(_)), "total")
        rez <- build(obj)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> $sum($literal(Bson.Int32(1))))),
            -\/($literal(Bson.Null)))))
    }

    "group in two projs" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        one   <- expr1(read)(κ($literal(Bson.Int32(1))))
        cp    =  makeObject(reduce(one)($sum(_)), "count")
        pop   <- lift(projectField(read, "pop"))
        total =  reduce(pop)($sum(_))
        tp    =  makeObject(total, "total")

        proj  <- objectConcat(cp, tp)
        rez   <- build(proj)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("count") -> $sum($literal(Bson.Int32(1))),
              BsonField.Name("total") -> $sum($var("pop")))),
            -\/($literal(Bson.Null)))))
    }

    "group on a field" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        pop     <- lift(projectField(read, "pop"))
        grouped =  groupBy(pop, List(city))
        total   =  reduce(grouped)($sum(_))
        obj     =  makeObject(total, "total")
        rez     <- build(obj)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> $sum($var("pop")))),
            -\/($var("city")))))
    }

    "group on a field, with un-grouped projection" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city    <- lift(projectField(read, "city"))
        grouped =  groupBy(read, List(city))
        city2   <- lift(projectField(grouped, "city"))
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)($sum(_))
        proj0   =  makeObject(total, "total")
        proj1   =  makeObject(city2, "city")
        projs   <- objectConcat(proj0, proj1)
        rez     <- build(projs)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("total") -> $sum($var("pop")),
              BsonField.Name("city")  -> $push($var("city")))),
            -\/($var("city"))),
          $unwind(DocField(BsonField.Name("city")))))
    }

    "group in expression" in {
      val read    = WorkflowBuilder.read(Collection("db", "zips"))
      val grouped = groupBy(read, List(pure(Bson.Int32(1))))
      val op = (for {
        pop     <- lift(projectField(grouped, "pop"))
        total   =  reduce(pop)($sum(_))
        expr    <- expr2(total, pure(Bson.Int32(1000)))($divide(_, _))
        inK     =  makeObject(expr, "totalInK")
        rez     <- build(inK)
      } yield rez).evalZero

      op must beRightDisjOrDiff(
        chain($read(Collection("db", "zips")),
          $group(
            Grouped(ListMap(
              BsonField.Name("__tmp2") -> $sum($var("pop")))),
            -\/($literal(Bson.Null))),
            $project(Reshape(ListMap(
              BsonField.Name("totalInK") ->
                -\/($divide($var("__tmp2"), $literal(Bson.Int32(1000)))))),
          IgnoreId)))
    }
  }

  "RenderTree[WorkflowBuilder]" should {
    def render(op: WorkflowBuilder)(implicit RO: RenderTree[WorkflowBuilder]):
        String =
      RO.render(op).draw.mkString("\n")

    val read = WorkflowBuilder.read(Collection("db", "zips"))

    "render in-process group" in {
      val grouped = groupBy(read, List(pure(Bson.Int32(1))))
      val op = for {
        pop <- projectField(grouped, "pop")
      } yield reduce(pop)($sum(_))
      op.map(render) must beRightDisj(
        """GroupBuilder
          |├─ ExprBuilder
          |│  ├─ CollectionBuilder
          |│  │  ├─ $Read(db; zips)
          |│  │  ├─ ExprOp($varF(DocVar.ROOT()))
          |│  │  ╰─ Schema(None)
          |│  ╰─ ExprOp($varF(DocField(BsonField.Name("pop"))))
          |├─ By
          |│  ╰─ ValueBuilder(Int32(1))
          |├─ Content
          |│  ╰─ \/-
          |│     ╰─ AccumOp($sum($varF(DocVar.ROOT())))
          |╰─ Id(9a1be37c)""".stripMargin)
    }

  }
}
