package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._
import scalaz._
import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.execute.PendingUntilFixed

@org.junit.runner.RunWith(classOf[org.specs2.runner.JUnitRunner])
class SemanticsSpec extends Specification {

  "TransformSelect" should {
    import slamdata.engine.SemanticAnalysis._
    import slamdata.engine.sql._
     
    val compiler = Compiler.trampoline

    def transform(q: Node): Option[Node] =
      SemanticAnalysis.TransformSelect(tree(q)).fold(e => None, tree => Some(tree.root))

    "add single field for order by" in {
      val q = SelectStmt(Proj(Ident("name"), None) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(
               SelectStmt(Proj(Ident("name"), None) :: Proj(Ident("height"), Some("__sd__0")) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("__sd__0"), ASC) :: Nil)),
                         None,
                         None)
               )
    }
    
    "not add a field that appears in the projections" in {
      val q = SelectStmt(Proj(Ident("name"), None) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }
    
    "not add a field that appears as an alias in the projections" in {
      val q = SelectStmt(Proj(Ident("foo"), Some("name")) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }
    
    "not add a field with wildcard present" in {
      val q = SelectStmt(Proj(Wildcard, None) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(q)
    }
    
    "add single field for order by" in {
      val q = SelectStmt(Proj(Ident("name"), None) :: Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("height"), ASC) :: 
                                       (Ident("name"), ASC) :: Nil)),
                         None,
                         None)
      transform(q) must beSome(
               SelectStmt(Proj(Ident("name"), None) :: 
                           Proj(Ident("height"), Some("__sd__0")) :: 
                           Nil, 
                         TableRelationAST("person", None) :: Nil,
                         None,
                         None,
                         Some(OrderBy((Ident("__sd__0"), ASC) :: 
                                       (Ident("name"), ASC) :: 
                                       Nil)),
                         None,
                         None)
               )
    }

  }
}
