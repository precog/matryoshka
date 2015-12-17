package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree
import quasar.fp._
import quasar.recursionschemes._, FunctorT.ops._
import quasar._; import Planner._
import quasar.javascript._
import quasar.specs2._

import org.specs2.execute.{Result}
import org.specs2.mutable._
import scalaz._, Scalaz._

class WorkflowBuilderSpec
    extends Specification
    with DisjunctionMatchers
    with TreeMatchers
    with PendingWithAccurateCoverage {
  import Workflow._
  import WorkflowBuilder._
  import IdHandling._
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._

  val readZips = WorkflowBuilder.read(Collection("db", "zips"))
  def pureInt(n: Int) = WorkflowBuilder.pure(Bson.Int32(n))

  "WorkflowBuilder" should {

    "make simple read" in {
      val op = build(read(Collection("db", "zips"))).evalZero

      op must beRightDisjunction($read(Collection("db", "zips")))
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
          BsonField.Name("city") -> \/-($field("city")))),
          IgnoreId)))
    }

    "make nested expression in single step" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        city  <- lift(projectField(read, "city"))
        state <- lift(projectField(read, "state"))
        x1    <- expr2(city, pure(Bson.Text(", ")))($concat(_, _))
        x2    <- expr2(x1, state)($concat(_, _))
        zero = makeObject(x2, "0")
      } yield zero).evalZero

      op must beRightDisjOrDiff(
        DocBuilder(
          WorkflowBuilder.read(Collection("db", "zips")),
          ListMap(BsonField.Name("0") ->
            \/-($concat($concat($field("city"), $literal(Bson.Text(", "))), $field("state"))))))
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
              \/-($concat($concat($field("city"), $literal(Bson.Text(", "))), $field("state"))))),
          List(ExprBuilder(
            WorkflowBuilder.read(Collection("db", "zips")),
            \/-($field("pop")))),
          { case f :: Nil => $match(Selector.Doc(f -> Selector.Lt(Bson.Int32(1000)))) }))
    }

    "combine array with constant value" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val pureArr = pure(Bson.Arr(List(Bson.Int32(0), Bson.Int32(1))))
      val op = (for {
        city   <- lift(projectField(read, "city"))
        array  <- arrayConcat(makeArray(city), pureArr)
        state2 <- lift(projectIndex(array, 2))
      } yield state2.transCata(normalize)).evalZero

      op must beRightDisjunction(ExprBuilder(read, $literal(Bson.Int32(1)).right))
    }

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

      op must beLeftDisjunction(UnsupportedFunction(
        quasar.std.StdLib.structural.ArrayProject,
        "array does not contain index ‘2’."))
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
            BsonField.Name("city") -> \/-($field("city")),
            BsonField.Name("pop")  -> \/-($field("pop")))),
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
      import jscore._

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
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "long" ->
            Access(Select(ident("x"), "loc"),
              Literal(Js.Num(1, false))),
          "public enemy #1" ->
            Access(Select(ident("x"), "enemies"),
              Literal(Js.Num(0, false))))))),
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
            BsonField.Name("__tmp0") -> $sum($field("pop")))),
          -\/(Reshape(ListMap(
            BsonField.Name("0") -> \/-($field("city")),
            BsonField.Name("1") -> \/-($field("state")))))),
        $project(
          Reshape(ListMap(
            BsonField.Name("value") -> \/-($field("__tmp0")))),
            ExcludeId)))
    }

    "distinct" in {
      val read = WorkflowBuilder.read(Collection("db", "zips"))
      val op = (for {
        proj <- lift(projectField(read, "city"))
        city =  makeObject(proj, "city")
        dist <- distinct(city)
        rez  <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
          $read(Collection("db", "zips")),
          $project(Reshape(ListMap(
            BsonField.Name("city") -> \/-($field("city")))),
            IgnoreId),
          $group(
            Grouped(ListMap(
              BsonField.Name("city") -> $first($field("city")))),
            -\/(Reshape(ListMap(BsonField.Name("0") -> \/-($field("city"))))))))
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
        dist    <- distinct(projs)
        rez     <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> $sum($$ROOT),
            BsonField.Name("city")  -> $push($field("city")))),
          -\/(Reshape(ListMap(BsonField.Name("0") -> \/-($field("city")))))),
        $unwind(DocField(BsonField.Name("city"))),
        $group(
          Grouped(ListMap(
            BsonField.Name("total") -> $first($field("total")),
            BsonField.Name("city")  -> $first($field("city")))),
          -\/(Reshape(ListMap(
            BsonField.Name("0") -> \/-($field("total")),
            BsonField.Name("1")  -> \/-($field("city"))))))))
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

        dist   <- distinct(lim)
        rez    <- build(dist)
      } yield rez).evalZero

      op must beRightDisjOrDiff(chain(
        $read(Collection("db", "zips")),
        $project(Reshape(ListMap(
          BsonField.Name("city") -> \/-($field("city")),
          BsonField.Name("state") -> \/-($field("state")))),
          IgnoreId),
        $sort(NonEmptyList(
          BsonField.Name("city") -> Ascending,
          BsonField.Name("state") -> Ascending)),
        $limit(10),
        $group(
          Grouped(ListMap(
            BsonField.Name("city") -> $first($field("city")),
            BsonField.Name("state") -> $first($field("state")))),
          -\/(Reshape(ListMap(
            BsonField.Name("0") -> \/-($field("city")),
            BsonField.Name("1") -> \/-($field("state")))))),
        $sort(NonEmptyList(
          BsonField.Name("city")  -> Ascending,
          BsonField.Name("state") -> Ascending))))
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
              BsonField.Name("total") -> $sum($field("pop")))),
            \/-($literal(Bson.Null)))))
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
            \/-($literal(Bson.Null)))))
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
              BsonField.Name("total") -> $sum($field("pop")))),
            \/-($literal(Bson.Null)))))
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
              BsonField.Name("total") -> $sum($field("pop")))),
            -\/(Reshape(ListMap(BsonField.Name("0") -> \/-($field("city"))))))))
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
              BsonField.Name("total") -> $sum($field("pop")),
              BsonField.Name("city")  -> $push($field("city")))),
            -\/(Reshape(ListMap(BsonField.Name("0") -> \/-($field("city")))))),
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
              BsonField.Name("__tmp2") -> $sum($field("pop")))),
            \/-($literal(Bson.Null))),
          $project(Reshape(ListMap(
            BsonField.Name("totalInK") ->
              \/-($divide($field("__tmp2"), $literal(Bson.Int32(1000)))))),
          IgnoreId)))
    }

    "normalize" should {
      val readFoo = CollectionBuilder($read(Collection("db", "foo")), Root(), None)

      "collapse simple reference to JS" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> -\/(jscore.JsFn(jscore.Name("x"), jscore.Literal(Js.Bool(true)))))),
          ListMap(
            BsonField.Name("0") -> \/-($var(DocField(BsonField.Name("__tmp"))))))
        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\/(jscore.JsFn(jscore.Name("y"), jscore.Literal(Js.Bool(true))))))

        normalize(w) must_== exp
      }

      "collapse reference in ExprOp" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \/-($var(DocField(BsonField.Name("foo")))))),
          ListMap(
            BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("__tmp")))))))
        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("foo")))))))

        normalize(w) must_== exp
      }

      "collapse reference to JS in ExprOp" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> -\/(jscore.JsFn(jscore.Name("x"), jscore.Literal(Js.Str("ABC")))))),
          ListMap(
            BsonField.Name("0") -> \/-($toLower($var(DocField(BsonField.Name("__tmp")))))))
        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\/(jscore.JsFn(jscore.Name("x"),
              jscore.Call(
                jscore.Select(jscore.Literal(Js.Str("ABC")), "toLowerCase"),
                List())))))

        normalize(w) must_== exp
      }

      "collapse reference through $$ROOT" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \/-($$ROOT))),
          ListMap(
            BsonField.Name("foo") -> \/-($var(DocField(BsonField.Name("__tmp") \ BsonField.Name("foo"))))))
        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("foo") -> \/-($var(DocField(BsonField.Name("foo"))))))

        normalize(w) must_== exp
      }

      "collapse JS reference" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp") -> \/-($var(DocField(BsonField.Name("foo")))))),
          ListMap(
            BsonField.Name("0") -> -\/(jscore.JsFn(jscore.Name("x"),
              jscore.Select(jscore.Select(jscore.ident("x"), "__tmp"), "length")))))

        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("0") -> -\/(jscore.JsFn(jscore.Name("y"),
              jscore.Select(jscore.Select(jscore.ident("y"), "foo"), "length")))))

        normalize(w) must_== exp
      }

      "collapse expression that contains a projection" in {
        val w = DocBuilderF(
          DocBuilder(
            readFoo,
            ListMap(
              BsonField.Name("__tmp0") -> \/-($subtract($var(DocField(BsonField.Name("pop"))), $literal(Bson.Int64(1)))))),

          ListMap(
            BsonField.Name("__tmp3") -> -\/(jscore.JsFn(jscore.Name("x"),
              jscore.Arr(List(jscore.Select(jscore.ident("x"), "__tmp0")))))))

        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("__tmp3") -> -\/(jscore.JsFn(jscore.Name("x"),
              jscore.Arr(List(jscore.BinOp(jscore.Sub,
                jscore.Select(jscore.ident("x"), "pop"),
                jscore.Literal(Js.Num(1, false)))))))))

        normalize(w) must_== exp
      }

      "collapse this" in {
        val w =
          DocBuilderF(
            DocBuilder(
              DocBuilder(
                readFoo,
                ListMap(
                  BsonField.Name("__tmp4") ->
                    \/-($and($lt($literal(Bson.Null), $field("pop")), $lt($field("pop"), $literal(Bson.Text(""))))),
                  BsonField.Name("__tmp5") -> \/-($$ROOT))),
              ListMap(
                BsonField.Name("__tmp6") ->
                  \/-($cond($field("__tmp4"), $field("__tmp5", "pop"), $literal(Bson.Null))),
                BsonField.Name("__tmp7") -> \/-($field("__tmp5")))),
            ListMap(
              BsonField.Name("__tmp8") -> \/-($field("__tmp7", "city")),
              BsonField.Name("__tmp9") -> \/-($field("__tmp6"))))

        val exp = DocBuilderF(
          readFoo,
          ListMap(
            BsonField.Name("__tmp8") -> \/-($field("city")),
            BsonField.Name("__tmp9") ->
              \/-($cond($and($lt($literal(Bson.Null), $field("pop")), $lt($field("pop"), $literal(Bson.Text("")))), $field("pop"), $literal(Bson.Null)))))

        normalize(w) must_== exp
      }
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
      op.map(render) must beRightDisjunction(
        """GroupBuilder(48712dc)
          |├─ ExprBuilder
          |│  ├─ CollectionBuilder(Root())
          |│  │  ├─ $Read(db; zips)
          |│  │  ╰─ Schema(None)
          |│  ╰─ ExprOp($varF(DocField(BsonField.Name("pop"))))
          |├─ By
          |│  ╰─ ValueBuilder(Int32(1))
          |╰─ Content
          |   ╰─ -\/
          |      ╰─ AccumOp($sum($varF(DocVar.ROOT())))""".stripMargin)
    }

  }
}
