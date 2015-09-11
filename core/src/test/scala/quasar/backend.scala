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
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("bar", None)),
        None, None, None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("./foo/bar", None)),
        None, None, None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-query table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(ExprRelationAST(
          Select(SelectAll,
            Proj(Splice(None), None) :: Nil,
            Some(TableRelationAST("bar", None)),
            None, None, None, None, None), "t")),
        None, None, None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(ExprRelationAST(
          Select(SelectAll,
            Proj(Splice(None), None) :: Nil,
            Some(TableRelationAST("./foo/bar", None)),
            None, None, None, None, None), "t")),
        None, None, None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make join table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(JoinRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(JoinRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None),
          LeftJoin,
          Ident("id")
        )),
        None, None, None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make cross table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(CrossRelation(
          TableRelationAST("bar", None),
          TableRelationAST("baz", None))),
        None, None, None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(CrossRelation(
          TableRelationAST("./foo/bar", None),
          TableRelationAST("./foo/baz", None))),
        None, None, None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }

    "make sub-select table names relative to base path" in {
      val q = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Proj(Ident("id"), None) :: Nil,
            Some(TableRelationAST("widget", None)),
            None, None, None, None, None),
          In)),
        None, None, None, None)
      val basePath = Path("foo/")
      val exp = Select(SelectAll,
        Proj(Splice(None), None) :: Nil,
        Some(TableRelationAST("./foo/bar", None)),
        Some(Binop(
          Ident("widgetId"),
          Select(SelectAll,
            Proj(Ident("id"), None) :: Nil,
            Some(TableRelationAST("./foo/widget", None)),
            None, None, None, None, None),
          In)),
        None, None, None, None)

      relativizePaths(q, basePath) must beRightDisjunction(exp)
    }
  }

}
