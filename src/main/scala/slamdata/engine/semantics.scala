package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.std.Library

import scalaz._
import scalaz.std.map._

import scalaz.syntax.apply._

trait SemanticAnalysis {
  import slamdata.engine.sql._
  import SemanticError._

  private type Failure = NonEmptyList[SemanticError]

  private def fail[A](e: SemanticError) = Validation.failure[NonEmptyList[SemanticError], A](NonEmptyList(e))
  private def succeed[A](s: A) = Validation.success[NonEmptyList[SemanticError], A](s)

  def tree(root: Node): AnnotatedTree[Node, Unit] = AnnotatedTree.unit(root, n => n.children)

  /**
   * This analyzer looks for function invocations (including operators), 
   * and binds them to their associated function definitions in the
   * provided library. If a function definition cannot be found, 
   * produces an error with details on the failure.
   */
  def FunctionBind[A](library: Library) = {
    def findFunction(name: String) = {
      library.functions.find(f => f.name == name).map(f => Validation.success(Some(f))).getOrElse(
        fail(FunctionNotFound(name))
      )
    }

    Analysis.annotate[Node, A, Option[Func], Failure] { 
      case (InvokeFunction(name, args)) => findFunction(name)          

      case (Unop(expr, op)) => findFunction(op.name)

      case (Binop(left, right, op)) => findFunction(op.name)

      case _ => Validation.success(None)
    }
  }

  final case class TableScope(scope: Map[String, SqlRelation])

  /**
   * This analysis identifies all the named tables within scope at each node in 
   * the tree. If two tables are given the same name within the same scope, then
   * because this leads to an ambiguity, an error is produced containing details
   * on the duplicate name.
   */
  def ScopeTables[A] = Analysis.readTree[Node, A, TableScope, Failure] { tree =>
    import Validation.{success, failure}

    Analysis.fork[Node, A, TableScope, Failure]((scopeOf, node) => {
      def parentScope(node: Node) = tree.parent(node).map(scopeOf).getOrElse(TableScope(Map()))

      node match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
          val parentMap = parentScope(node).scope

          (relations.foldLeft[Validation[Failure, Map[String, SqlRelation]]](success(Map.empty[String, SqlRelation])) {
            case (v, relation) =>
              implicit val sg = Semigroup.firstSemigroup[SqlRelation]

              v +++ tree.subtree(relation).foldDown[Validation[Failure, Map[String, SqlRelation]]](success(Map.empty[String, SqlRelation])) {
                case (v, relation : SqlRelation) =>

                  v.fold(
                    failure,
                    acc => {
                      val name = relation match {
                        case r @ TableRelationAST(name, aliasOpt) => Some(aliasOpt.getOrElse(name))
                        case r @ SubqueryRelationAST(subquery, alias) => Some(alias)
                        case r @ JoinRelation(left, right, join, clause) => None
                      }

                      (name.map { name =>
                        (acc.get(name).map{ relation2 =>
                          failure(NonEmptyList(DuplicateRelationName(name, relation2)))
                        }).getOrElse(success(acc + (name -> relation)))
                      }).getOrElse(success(acc))
                    }
                  )

                case (v, _) => v // We're only interested in relations                
              }
          }).map(map => TableScope(parentMap ++ map))

        case _ => success(parentScope(node))
      }
    })
  }

  /* sealed trait Provenance
  object Provenance {
    case class Table(value: String) extends Provenance
    case class Either(left: provenance, right: Provenance) extends Provenance
    case class Both(left: Provenance, right: Provenance) extends Provenance
  }

  def ProvenanceInfer = Analysis.fork[Node, TableScope, Provenance, Failure]((provOf, node) => {
    ???
  }) */

  /**
   * This analyzer works bottom-up to infer the type of all expressions.
   * If a type is inferred to have contradictory constraints, a type 
   * error will be produced that contains details on the contradiction.
   */
  def TypeInfer = {
    Analysis.readTree[Node, Option[Func], Type, Failure] { tree =>
      Analysis.join[Node, Option[Func], Type, Failure]((typeOf, node) => {
        def succeed(v: Type): ValidationNel[SemanticError, Type] = Validation.success(v)

        def fail(error: SemanticError): ValidationNel[SemanticError, Type] = Validation.failure(NonEmptyList(error))

        def func(node: Node): ValidationNel[SemanticError, Func] = {
          tree.attr(node).map(Validation.success).getOrElse(Validation.failure(NonEmptyList(FunctionNotBound(node))))
        }

        def NA = succeed(Type.Bottom)

        def propagate(n: Node) = succeed(typeOf(n))

        node match {
          case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
            // TODO: Use object instead of array so we can hang onto names:
            succeed(Type.makeArray(projections.map(typeOf)))

          case Proj(expr, alias) => propagate(expr)

          case Subselect(select) => propagate(select)

          case SetLiteral(values) => succeed(Type.makeArray(values.map(typeOf)))

          case Wildcard => succeed(Type.Top)

          case Binop(left, right, op) =>
            func(node).fold(
              Validation.failure,
              _.codomain(typeOf(left), typeOf(right))
            )

          case Unop(expr, op) =>
            func(node).fold(
              Validation.failure,
              _.codomain(typeOf(expr))
            )

          case Ident(name) => ???

          case InvokeFunction(name, args) =>
            func(node).fold(
              Validation.failure,
              _.codomain(args.toList.map(typeOf))
            )

          case Case(cond, expr) => succeed(typeOf(expr))

          case Match(expr, cases, default) => 
            succeed(cases.map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)

          case Switch(cases, default) => 
            succeed(cases.map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)

          case IntLiteral(value) => succeed(Type.Const(Data.Int(value)))

          case FloatLiteral(value) => succeed(Type.Const(Data.Dec(value)))

          case StringLiteral(value) => succeed(Type.Const(Data.Str(value)))

          case NullLiteral() => succeed(Type.Const(Data.Null))

          case TableRelationAST(name, alias) => NA

          case SubqueryRelationAST(subquery, alias) => propagate(subquery)

          case JoinRelation(left, right, tpe, clause) => succeed(Type.Bool)

          case GroupBy(keys, having) => succeed(Type.makeArray(keys.map(typeOf)))

          case OrderBy(keys) => NA

          case _ : BinaryOperator => NA

          case _ : UnaryOperator => NA
        }
      })
    }
  }

}
object SemanticAnalysis extends SemanticAnalysis