package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._
import scalaz._
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import slamdata.specs2._

@org.junit.runner.RunWith(classOf[org.specs2.runner.JUnitRunner])
class SemanticsSpec extends Specification with PendingWithAccurateCoverage {

  "TransformSelect" should {
    import slamdata.engine.SemanticAnalysis._
    import slamdata.engine.sql._

    val compiler = Compiler.trampoline

    def transform(q: Node): Option[Node] =
      SemanticAnalysis.TransformSelect(tree(q)).fold(e => None, tree => Some(tree.root))

    "add single field for order by" in {
      val q = SelectStmt(SelectAll,
                         Proj.Anon(Ident("name")) :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(
               SelectStmt(SelectAll,
                         Proj.Anon(Ident("name")) :: Proj.Named(Ident("height"), "__sd__0") :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("__sd__0"), ASC) :: Nil)),
                         None,
                         None)
               )
    }

    "not add a field that appears in the projections" in {
      val q = SelectStmt(SelectAll,
                         Proj.Anon(Ident("name")) :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }

    "not add a field that appears as an alias in the projections" in {
      val q = SelectStmt(SelectAll,
                         Proj.Named(Ident("foo"), "name") :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }

    "not add a field with wildcard present" in {
      val q = SelectStmt(SelectAll,
                         Proj.Anon(Splice(None)) :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }

    "add single field for order by" in {
      val q = SelectStmt(SelectAll,
                         Proj.Anon(Ident("name")) :: Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) ::
                                       (Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(
               SelectStmt(SelectAll,
                         Proj.Anon(Ident("name")) ::
                           Proj.Named(Ident("height"), "__sd__0") ::
                           Nil,
                         Some(TableRelationAST("person", None)),
                         None,
                         None,
                         Some(OrderBy((Ident("__sd__0"), ASC) ::
                                       (Ident("name"), ASC) ::
                                       Nil)),
                         None,
                         None)
               )
    }

    "transform sub-select" in {
      val q = SelectStmt(SelectAll,
                         Proj.Anon(Splice(None)) :: Nil,
                         Some(TableRelationAST("foo", None)),
                         Some(
                           Binop(
                             Ident("a"),
                             Subselect(
                               SelectStmt(SelectAll,
                                          Proj.Anon(Ident("a")) :: Nil,
                                          Some(TableRelationAST("bar", None)),
                                          None,
                                          None,
                                          Some(OrderBy((Ident("b"), ASC) :: Nil)),
                                          None,
                                          None)
                             ),
                             In)
                         ),
                         None,
                         None,
                         None,
                         None)
      transform(q) must beSome(
              SelectStmt(SelectAll,
                         Proj.Anon(Splice(None)) :: Nil,
                         Some(TableRelationAST("foo", None)),
                         Some(
                           Binop(
                             Ident("a"),
                             Subselect(
                               SelectStmt(SelectAll,
                                          Proj.Anon(Ident("a")) ::
                                            Proj.Named(Ident("b"), "__sd__0") ::
                                            Nil,
                                          Some(TableRelationAST("bar", None)),
                                          None,
                                          None,
                                          Some(OrderBy((Ident("__sd__0"), ASC) :: Nil)),
                                          None,
                                          None)
                             ),
                             In)
                         ),
                         None,
                         None,
                         None,
                         None)
      )
    }.pendingUntilFixed

  }
}
