package quasar

import quasar.Predef._
import quasar.sql.SQLParser
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import quasar.specs2._

class SemanticsSpec extends Specification with PendingWithAccurateCoverage {

  "TransformSelect" should {
    import quasar.SemanticAnalysis._
    import quasar.sql._

    val compiler = Compiler.trampoline

    def transform(q: Expr): Option[Expr] =
      SemanticAnalysis.TransformSelect(tree(q)).fold(e => None, tree => Some(tree.root))

    "add single field for order by" in {
      val q = Select(SelectAll,
                     Proj(Ident("name"), None) :: Nil,
                     Some(TableRelationAST("person", None)),
                     None,
                     None,
                     Some(OrderBy((ASC, Ident("height")) :: Nil)),
                     None,
                     None)
      transform(q) must beSome(
               Select(SelectAll,
                      Proj(Ident("name"), None) :: Proj(Ident("height"), Some("__sd__0")) :: Nil,
                      Some(TableRelationAST("person", None)),
                      None,
                      None,
                      Some(OrderBy((ASC, Ident("__sd__0")) :: Nil)),
                      None,
                      None)
               )
    }

    "not add a field that appears in the projections" in {
      val q = Select(SelectAll,
                     Proj(Ident("name"), None) :: Nil,
                     Some(TableRelationAST("person", None)),
                     None,
                     None,
                     Some(OrderBy((ASC, Ident("name")) :: Nil)),
                     None,
                     None)
      transform(q) must beSome(q)
    }

    "not add a field that appears as an alias in the projections" in {
      val q = Select(SelectAll,
                     Proj(Ident("foo"), Some("name")) :: Nil,
                     Some(TableRelationAST("person", None)),
                     None,
                     None,
                     Some(OrderBy((ASC, Ident("name")) :: Nil)),
                     None,
                     None)
      transform(q) must beSome(q)
    }

    "not add a field with wildcard present" in {
      val q = Select(SelectAll,
                     Proj(Splice(None), None) :: Nil,
                     Some(TableRelationAST("person", None)),
                     None,
                     None,
                     Some(OrderBy((ASC, Ident("height")) :: Nil)),
                     None,
                     None)
      transform(q) must beSome(q)
    }

    "add single field for order by" in {
      val q = Select(SelectAll,
                     Proj(Ident("name"), None) :: Nil,
                     Some(TableRelationAST("person", None)),
                     None,
                     None,
                     Some(OrderBy((ASC, Ident("height")) ::
                                  (ASC, Ident("name")) ::
                                  Nil)),
                     None,
                     None)
      transform(q) must beSome(
               Select(SelectAll,
                      Proj(Ident("name"), None) ::
                        Proj(Ident("height"), Some("__sd__0")) ::
                        Nil,
                      Some(TableRelationAST("person", None)),
                      None,
                      None,
                      Some(OrderBy((ASC, Ident("__sd__0")) ::
                                   (ASC, Ident("name")) ::
                                   Nil)),
                      None,
                      None))
    }

    "transform sub-select" in {
      val q = Select(SelectAll,
                     Proj(Splice(None), None) :: Nil,
                     Some(TableRelationAST("foo", None)),
                     Some(
                       Binop(
                         Ident("a"),
                         Select(SelectAll,
                                Proj(Ident("a"), None) :: Nil,
                                Some(TableRelationAST("bar", None)),
                                None,
                                None,
                                Some(OrderBy((ASC, Ident("b")) :: Nil)),
                                None,
                                None),
                         In)),
                     None,
                     None,
                     None,
                     None)
      transform(q) must beSome(
              Select(SelectAll,
                     Proj(Splice(None), None) :: Nil,
                     Some(TableRelationAST("foo", None)),
                     Some(
                       Binop(
                         Ident("a"),
                         Select(SelectAll,
                                Proj(Ident("a"), None) ::
                                  Proj(Ident("b"), Some("__sd__0")) ::
                                  Nil,
                                Some(TableRelationAST("bar", None)),
                                None,
                                None,
                                Some(OrderBy((ASC, Ident("__sd__0")) :: Nil)),
                                None,
                                None),
                         In)),
                     None,
                     None,
                     None,
                     None))
    }.pendingUntilFixed

  }
}
