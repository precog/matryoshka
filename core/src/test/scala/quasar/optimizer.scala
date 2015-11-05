package quasar

import quasar.Predef._
import quasar.fs._
import quasar.recursionschemes._, FunctorT.ops._
import quasar.std._

import org.specs2.mutable._

class OptimizerSpec extends Specification with CompilerHelpers with TreeMatchers {
  import StdLib._
  import structural._
  import set._

  import LogicalPlan._

  "simplify" should {

    "inline trivial binding" in {
      Optimizer.simplifyƒ[Fix].apply(LetF('tmp0, read("foo"), Free('tmp0))) must
        beSome(ReadF[Fix[LogicalPlan]](fs.Path("foo")))
    }

    "not inline binding that's used twice" in {
      Optimizer.simplifyƒ[Fix].apply(
        LetF('tmp0, read("foo"),
          makeObj(
            "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
            "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))) must
        beNone
    }

    "completely inline stupid lets" in {
      Optimizer.simplifyƒ[Fix].apply(
        LetF('tmp0, read("foo"), Let('tmp1, Free('tmp0), Free('tmp1)))) must
        beSome(LetF('tmp1, read("foo"), Free('tmp1)))
    }

    "inline correct value for shadowed binding" in {
      Optimizer.simplifyƒ[Fix].apply(
        LetF('tmp0, read("foo"),
          Let('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))) must
        beSome(
          LetF('tmp0, read("bar"),
            makeObj(
              "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))
    }

    "inline a binding used once, then shadowed once" in {
      Optimizer.simplifyƒ[Fix].apply(
        LetF('tmp0, read("foo"),
          ObjectProject(Free('tmp0),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar")))))))) must
        beSome(
          InvokeF(ObjectProject, List(
            read("foo"),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))))))))
    }

    "inline a binding used once, then shadowed twice" in {
      Optimizer.simplifyƒ[Fix].apply(
        LetF('tmp0, read("foo"),
          ObjectProject(Free('tmp0),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz")))))))) must
        beSome(
          InvokeF(ObjectProject, List(
            read("foo"),
            Let('tmp0, read("bar"),
              makeObj(
                "bar" -> ObjectProject(Free('tmp0), Constant(Data.Str("bar"))),
                "baz" -> ObjectProject(Free('tmp0), Constant(Data.Str("baz"))))))))
    }

    "partially inline a more interesting case" in {
      Let('tmp0, read("person"),
        Let('tmp1,
          makeObj(
            "name" -> ObjectProject(Free('tmp0), Constant(Data.Str("name")))),
          Let('tmp2,
            OrderBy(
              Free('tmp1),
              MakeArray(
                ObjectProject(Free('tmp1), Constant(Data.Str("name"))))),
            Free('tmp2)))).transCata(repeatedly(Optimizer.simplifyƒ)) must_==
        Let('tmp1,
          makeObj(
            "name" ->
              ObjectProject(read("person"), Constant(Data.Str("name")))),
          OrderBy(
            Free('tmp1),
            MakeArray(ObjectProject(Free('tmp1), Constant(Data.Str("name"))))))
    }
  }

  "preferProjections" should {
    "ignore a delete with unknown shape" in {
      Optimizer.preferProjections(
        DeleteField(Read(Path.fileRel("zips")),
          Constant(Data.Str("pop")))) must
        beTree(
          DeleteField(Read(Path.fileRel("zips")),
            Constant(Data.Str("pop"))))
    }

    "convert a delete after a projection" in {
      Optimizer.preferProjections(
        Let('meh, Read(Path.fileRel("zips")),
          DeleteField(
            MakeObjectN(
              Constant(Data.Str("city")) ->
                ObjectProject(Free('meh), Constant(Data.Str("city"))),
              Constant(Data.Str("pop")) ->
                ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            Constant(Data.Str("pop"))))) must
      beTree(
        Let('meh, Read(Path.fileRel("zips")),
          MakeObjectN(
            Constant(Data.Str("city")) ->
              ObjectProject(MakeObjectN(
                Constant(Data.Str("city")) ->
                  ObjectProject(Free('meh), Constant(Data.Str("city"))),
                Constant(Data.Str("pop")) ->
                  ObjectProject(Free('meh), Constant(Data.Str("pop")))),
                Constant(Data.Str("city"))))))
    }

    "convert a delete when the shape is hidden by a Free" in {
      Optimizer.preferProjections(
        Let('meh, Read(Path.fileRel("zips")),
          Let('meh2,
            MakeObjectN(
              Constant(Data.Str("city")) ->
                ObjectProject(Free('meh), Constant(Data.Str("city"))),
              Constant(Data.Str("pop")) ->
                ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            MakeObjectN(
              Constant(Data.Str("orig")) -> Free('meh2),
              Constant(Data.Str("cleaned")) ->
                DeleteField(Free('meh2), Constant(Data.Str("pop"))))))) must
      beTree(
        Let('meh, Read(Path.fileRel("zips")),
          Let('meh2,
            MakeObjectN(
              Constant(Data.Str("city")) ->
                ObjectProject(Free('meh), Constant(Data.Str("city"))),
              Constant(Data.Str("pop")) ->
                ObjectProject(Free('meh), Constant(Data.Str("pop")))),
            MakeObjectN(
              Constant(Data.Str("orig")) -> Free('meh2),
              Constant(Data.Str("cleaned")) ->
                MakeObjectN(
                  Constant(Data.Str("city")) ->
                    ObjectProject(Free('meh2), Constant(Data.Str("city"))))))))
    }
  }
}
