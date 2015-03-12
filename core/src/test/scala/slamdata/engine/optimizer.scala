package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.std._

import org.specs2.mutable._

class OptimizerSpec extends Specification with CompilerHelpers {
  import StdLib._
  import structural._
  import set._

  import LogicalPlan._

  "simplify" should {

    "inline trivial binding" in {
      val lp = Let('tmp0, read("foo"), Free('tmp0))

      lp.cata(Optimizer.simplify) should_== read("foo")
    }

    "not inline binding that's used twice" in {
      val lp =
        Let('tmp0, read("foo"),
          makeObj(
            "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
            "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))

      lp.cata(Optimizer.simplify) should_== lp
    }

    "completely inline stupid lets" in {
      val lp =
        Let('tmp0, read("foo"),
          Let('tmp1, Free('tmp0), // OK, this one is pretty silly
            Free('tmp1)))

      lp.cata(Optimizer.simplify) should_== read("foo")
    }

    "inline correct value for shadowed binding" in {
      val lp =
        Let('tmp0, read("foo"),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))

      lp.cata(Optimizer.simplify) should_==
        makeObj("bar" -> ObjectProject(read("bar"), Constant(Data.Str("bar"))))
    }

    "inline a binding used once, then shadowed once" in {
      val lp =
        Let('tmp0, read("foo"),
          ObjectProject(Free('tmp0),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))))))

      lp.cata(Optimizer.simplify) should_==
          ObjectProject(read("foo"),
            makeObj(
              "bar" -> ObjectProject(read("bar"), Constant(Data.Str("bar")))))
    }

    "inline a binding used once, then shadowed twice" in {
      val lp =
        Let('tmp0, read("foo"),
          ObjectProject(Free('tmp0),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))))

      lp.cata(Optimizer.simplify) should_==
          ObjectProject(read("foo"),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))
    }

    "partially inline a more interesting case" in {
      val lp =
        Let('tmp0, read("person"),
          Let('tmp1,
            makeObj(
              "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name")))),
            Let('tmp2,
              OrderBy(
                Free('tmp1),
                MakeArray(
                  ObjectProject(Free('tmp1), Constant(Data.Str("name"))))),
              Free('tmp2))))

      val slp =
        Let('tmp1,
          makeObj(
            "name" ->
              ObjectProject(read("person"), Constant(Data.Str("name")))),
          OrderBy(
            Free('tmp1),
            MakeArray(ObjectProject(Free('tmp1), Constant(Data.Str("name"))))))

      lp.cata(Optimizer.simplify) should_== slp
    }

  }

}
