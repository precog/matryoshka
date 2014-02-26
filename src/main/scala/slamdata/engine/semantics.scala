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
      val lcase = name.toLowerCase

      library.functions.find(f => f.name.toLowerCase == lcase).map(f => Validation.success(Some(f))).getOrElse(
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

  sealed trait Provenance {
    def & (that: Provenance): Provenance = Provenance.Both(this, that)

    def | (that: Provenance): Provenance = Provenance.Either(this, that)
  }
  object Provenance {
    case object Unknown extends Provenance
    case object Value extends Provenance
    case class Relation(value: SqlRelation) extends Provenance
    case class Either(left: Provenance, right: Provenance) extends Provenance
    case class Both(left: Provenance, right: Provenance) extends Provenance

    def allOf(xs: Seq[Provenance]): Provenance = reduce(xs)(Both.apply _)

    def anyOf(xs: Seq[Provenance]): Provenance = reduce(xs)(Either.apply _)

    private def reduce(xs: Seq[Provenance])(f: (Provenance, Provenance) => Provenance): Provenance = {
      if (xs.length == 0) Unknown
      else if (xs.length == 1) xs.head
      else xs.reduce(f) 
    }
  }

  /**
   * This phase infers the provenance of every expression, issuing errors
   * if identifiers are used with unknown provenance. The phase requires
   * TableScope annotations on the tree.
   */
  def ProvenanceInfer = Analysis.readTree[Node, TableScope, Provenance, Failure] { tree =>
    Analysis.join[Node, TableScope, Provenance, Failure]((provOf, node) => {
      import Validation.{success, failure}

      def propagate(n: Node) = success(provOf(n))

      def NA = success(Provenance.Unknown)

      node match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
          success(Provenance.allOf(relations.map(provOf)))

        case Proj(expr, alias) => propagate(expr)

        case Subselect(select) => propagate(select)

        case SetLiteral(values) => success(Provenance.Value)

        case Wildcard => NA // FIXME

        case Binop(left, right, op) => success(provOf(left) & provOf(right))

        case Unop(expr, op) => success(provOf(expr))

        case ident @ Ident(name) => 
          val tableScope = tree.attr(node).scope

          (tableScope.get(name).map((Provenance.Relation.apply _) andThen success)).getOrElse {
            Provenance.anyOf(tableScope.values.toSeq.map(Provenance.Relation.apply)) match {
              case Provenance.Unknown => failure(NonEmptyList(NoTableDefined(ident)))

              case x => success(x)
            }
          }

        case InvokeFunction(name, args) => success(Provenance.allOf(args.map(provOf)))

        case Case(cond, expr) => propagate(expr)

        case Match(expr, cases, default) => success(cases.map(provOf).reduce(_ & _))

        case Switch(cases, default) => success(cases.map(provOf).reduce(_ & _))

        case IntLiteral(value) => success(Provenance.Value)

        case FloatLiteral(value) => success(Provenance.Value)

        case StringLiteral(value) => success(Provenance.Value)

        case NullLiteral() => success(Provenance.Value)

        case r @ TableRelationAST(name, alias) => success(Provenance.Relation(r))

        case r @ SubqueryRelationAST(subquery, alias) => success(Provenance.Relation(r))

        case r @ JoinRelation(left, right, tpe, clause) => success(Provenance.Relation(r))

        case GroupBy(keys, having) => success(Provenance.allOf(keys.map(provOf)))

        case OrderBy(keys) => success(Provenance.allOf(keys.map(_._1).map(provOf)))

        case _ : BinaryOperator => NA

        case _ : UnaryOperator => NA
      }
    })
  }

  /**
   * This phase works top-down to push out known types to types with unknowable
   * types (such as columns and wildcards).
   */
  def TypeInfer = Analysis.readTree[Node, Option[Func], Type, Failure] { tree =>
    Analysis.fork[Node, Option[Func], Type, Failure]((typeOf, node) => {
      ???
    })
  }

  /**
   * This phase works bottom-up to check the type of all expressions.
   * In the event of a type error, an error will be produced containing
   * details on the expected versus actual type.
   */
  def TypeCheck = {
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