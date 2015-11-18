package quasar

import quasar.Predef._

import org.specs2.mutable._
import org.specs2.scalaz._

class BackendSpecs extends Specification with DisjunctionMatchers {
  import quasar.sql._

  "interpretPaths" should {
    import quasar.fs.{Path}

    "make simple table name relative to base path" in {
      val q = Select(SelectAll,
        Row(List(Splice(None))),
        Some(TableRelationAST("bar", None)),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Row(List(Splice(None))),
        Some(TableRelationAST("./foo/bar", None)),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-query table names relative to base path" in {
      val q = Select(SelectAll,
        Row(List(Splice(None))),
        Some(ExprRelationAST(
          Select(SelectAll,
            Row(List(Splice(None))),
            Some(TableRelationAST("bar", None)),
            None, None, None), "t")),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Row(List(Splice(None))),
        Some(ExprRelationAST(
          Select(SelectAll,
            Row(List(Splice(None))),
            Some(TableRelationAST("./foo/bar", None)),
            None, None, None), "t")),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make join table names relative to base path" in {
      val q = Select(SelectAll,
        Row(List(Splice(None))),
        Some(JoinRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Row(List(Splice(None))),
        Some(JoinRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make cross table names relative to base path" in {
      val q = Select(SelectAll,
        Row(List(Splice(None))),
        Some(CrossRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None))),
        None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Row(List(Splice(None))),
        Some(CrossRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None))),
        None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-select table names relative to base path" in {
      val q = Select(SelectAll,
        Row(List(Splice(None))),
        Some(TableRelationAST("bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Row(List(Ident("id"))),
            Some(TableRelationAST("widget", None)),
            None, None, None),
          In)),
        None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Row(List(Splice(None))),
        Some(TableRelationAST("./foo/bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Row(List(Ident("id"))),
            Some(TableRelationAST("./foo/widget", None)),
            None, None, None),
          In)),
        None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }
  }

}
