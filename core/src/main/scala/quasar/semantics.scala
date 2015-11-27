/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.recursionschemes._, Recursive.ops._
import quasar.sql._

import scala.AnyRef

import scalaz._, Scalaz._, Validation.{success, failure}
import shapeless.contrib.scalaz._

sealed trait SemanticError {
  def message: String
}

object SemanticError {
  implicit val SemanticErrorShow: Show[SemanticError] = Show.shows(_.message)

  final case class GenericError(message: String) extends SemanticError

  final case class DomainError(data: Data, hint: Option[String]) extends SemanticError {
    def message = "The data '" + data + "' did not fall within its expected domain" + hint.map(": " + _)
  }

  final case class FunctionNotFound(name: String) extends SemanticError {
    def message = "The function '" + name + "' could not be found in the standard library"
  }
  final case class FunctionNotBound(node: Expr) extends SemanticError {
    def message = "A function was not bound to the node " + node
  }
  final case class TypeError(expected: Type, actual: Type, hint: Option[String]) extends SemanticError {
    def message = "Expected type " + expected + " but found " + actual + hint.map(": " + _).getOrElse("")
  }
  final case class VariableParseError(vari: VarName, value: VarValue, cause: quasar.sql.ParsingError) extends SemanticError {
    def message = "The variable " + vari + " should contain a SQL expression but was `" + value.value + "` (" + cause.message + ")"
  }
  final case class UnboundVariable(vari: VarName) extends SemanticError {
    def message = "There is no binding for the variable " + vari
  }
  final case class DuplicateRelationName(defined: String) extends SemanticError {
    def message = "Found relation with duplicate name '" + defined + "'"
  }
  final case class NoTableDefined(node: Expr) extends SemanticError {
    def message = "No table was defined in the scope of \'" + pprint(node) + "\'"
  }
  final case class MissingField(name: String) extends SemanticError {
    def message = "No field named '" + name + "' exists"
  }
  final case class MissingIndex(index: Int) extends SemanticError {
    def message = "No element exists at array index '" + index
  }
  final case class WrongArgumentCount(func: Func, expected: Int, actual: Int) extends SemanticError {
    def message = "Wrong number of arguments for function '" + func.name + "': expected " + expected + " but found " + actual
  }
  final case class ExpectedLiteral(node: Expr) extends SemanticError {
    def message = "Expected literal but found '" + pprint(node) + "'"
  }
  final case class AmbiguousReference(node: Expr, relations: List[SqlRelation[Expr]]) extends SemanticError {
    def message = "The expression '" + pprint(node) + "' is ambiguous and might refer to any of the tables " + relations.mkString(", ")
  }
  final case object CompiledTableMissing extends SemanticError {
    def message = "Expected the root table to be compiled but found nothing"
  }
  final case class CompiledSubtableMissing(name: String) extends SemanticError {
    def message = "Expected to find a compiled subtable with name \"" + name + "\""
  }
  final case class DateFormatError(func: Func, str: String, hint: Option[String]) extends SemanticError {
    def message = "Date/time string could not be parsed as " + func.name + ": " + str + hint.map(" (" + _ + ")").getOrElse("")
  }
}

trait SemanticAnalysis {
  import SemanticError._

  type Failure = NonEmptyList[SemanticError]

  private def fail[A](e: SemanticError) = Validation.failure[Failure, A](NonEmptyList(e))
  private def succeed[A](s: A) = Validation.success[Failure, A](s)

  sealed trait Synthetic
  object Synthetic {
    final case object SortKey extends Synthetic

    implicit val SyntheticRenderTree: RenderTree[Synthetic] =
      RenderTree.fromToString[Synthetic]("Synthetic")
  }

  private val syntheticPrefix = "__sd__"

  /** Inserts synthetic fields into the projections of each `select` stmt to
    * hold the values that will be used in sorting, and annotates each new
    * projection with Synthetic.SortKey. The compiler will generate a step to
    * remove these fields after the sort operation.
    */
  val projectSortKeysƒ: ExprF[Expr] => Expr = {
    case SelectF(d, projections, r, f, g, Some(sql.OrderBy(keys))) => {
      def matches(key: Expr): PartialFunction[Proj[Expr], Expr] = key match {
        case Ident(keyName) => {
          case Proj(_, Some(alias))        if keyName == alias    => key
          case Proj(Ident(projName), None) if keyName == projName => key
          case Proj(Splice(_), _)                                 => key
        }
        case _ => {
          case Proj(expr2, Some(alias)) if key == expr2 => Ident(alias)
        }
      }

      // NB: order of the keys has to be preserved, so this complex fold
      //     seems to be the best way.
      type Target = (List[Proj[Expr]], List[(OrderType, Expr)], Int)

      val (projs2, keys2, _) = keys.foldRight[Target]((Nil, Nil, 0)) {
        case ((orderType, expr), (projs, keys, index)) =>
          projections.collectFirst(matches(expr)).fold {
            val name  = syntheticPrefix + index.toString()
            val proj2 = Proj(expr, Some(name))
            val key2  = (orderType, Ident(name))
            (proj2 :: projs, key2 :: keys, index + 1)
          } (
            kExpr => (projs, (orderType, kExpr) :: keys, index))
      }
      Select(d, projections ⊹ projs2, r, f, g, Some(sql.OrderBy(keys2)))
    }
    case node => Fix(node)
  }

  private val identifySyntheticsƒ:
      ExprF[List[Option[Synthetic]]] => List[Option[Synthetic]] = {
    case SelectF(_, projections, _, _, _, _) =>
      projections.map(_.alias match {
        case Some(name) if name.startsWith(syntheticPrefix) =>
          Some(Synthetic.SortKey)
        case _ => None
      })
    case _ => Nil
  }

  case class TableScope(scope: Map[String, SqlRelation[Expr]])

  implicit val ShowTableScope: Show[TableScope] = new Show[TableScope] {
    override def show(v: TableScope) = v.scope.toString
  }

  import Validation.FlatMap._

  type ValidSem[A] = ValidationNel[SemanticError, A]

  /** This analysis identifies all the named tables within scope at each node in
    * the tree. If two tables are given the same name within the same scope,
    * then because this leads to an ambiguity, an error is produced containing
    * details on the duplicate name.
    */
  val scopeTablesƒ: (TableScope, Expr) => ValidSem[ExprF[(TableScope, Expr)]] =
    (parentScope, expr) => expr.unFix match {
      case sel @ SelectF(_, _, relations, _, _, _) =>
        def findRelations(r: SqlRelation[Expr]): ValidSem[Map[String, SqlRelation[Expr]]] =
          r match {
            case TableRelationAST(name, aliasOpt) =>
              success(Map(aliasOpt.getOrElse(name) -> r))
            case ExprRelationAST(_, alias) => success(Map(alias -> r))
            case JoinRelation(l, r, _, _) => for {
              rels <- findRelations(l) tuple findRelations(r)
              (left, right) = rels
              rez <- (left.keySet intersect right.keySet).toList match {
                case Nil           => success(left ++ right)
                case con :: flicts =>
                  failure(NonEmptyList.nel(con, flicts).map(DuplicateRelationName(_)))
              }
            } yield rez
          }

        relations.fold[ValidSem[Map[String, SqlRelation[Expr]]]](
          success(Map[String, SqlRelation[Expr]]()))(
          findRelations)
          .map(m => sel.map((TableScope(m), _)))
      case x => success(x.map((parentScope, _)))
    }

  sealed trait Provenance {
    import Provenance._

    def & (that: Provenance): Provenance = Both(this, that)

    def | (that: Provenance): Provenance = Either(this, that)

    def simplify: Provenance = this match {
      case x : Either => anyOf(x.flatten.map(_.simplify).filterNot(_ == Empty))
      case x : Both => allOf(x.flatten.map(_.simplify).filterNot(_ == Empty))
      case _ => this
    }

    def namedRelations: Map[String, List[NamedRelation[Expr]]] =
      relations.foldMap(_.namedRelations)

    def relations: List[SqlRelation[Expr]] = this match {
      case Empty => Nil
      case Value => Nil
      case Relation(value) => value :: Nil
      case Either(v1, v2) => v1.relations ++ v2.relations
      case Both(v1, v2) => v1.relations ++ v2.relations
    }

    def flatten: Set[Provenance] = Set(this)

    override def equals(that: scala.Any): Boolean = (this, that) match {
      case (x, y) if (x.eq(y.asInstanceOf[AnyRef])) => true
      case (Relation(v1), Relation(v2)) => v1 == v2
      case (Either(_, _), that @ Either(_, _)) => this.simplify.flatten == that.simplify.flatten
      case (Both(_, _), that @ Both(_, _)) => this.simplify.flatten == that.simplify.flatten
      case (_, _) => false
    }

    override def hashCode = this match {
      case Either(_, _) => this.simplify.flatten.hashCode
      case Both(_, _) => this.simplify.flatten.hashCode
      case _ => super.hashCode
    }
  }
  trait ProvenanceInstances {
    implicit val ProvenanceRenderTree: RenderTree[Provenance] =
      new RenderTree[Provenance] { self =>
        import Provenance._

        def render(v: Provenance) = {
          val ProvenanceNodeType = List("Provenance")

          def nest(l: RenderedTree, r: RenderedTree, sep: String) = (l, r) match {
            case (RenderedTree(_, ll, Nil), RenderedTree(_, rl, Nil)) =>
              Terminal(ProvenanceNodeType, Some("(" + ll + " " + sep + " " + rl + ")"))
            case _ => NonTerminal(ProvenanceNodeType, Some(sep), l :: r :: Nil)
          }

          v match {
            case Empty               => Terminal(ProvenanceNodeType, Some("Empty"))
            case Value               => Terminal(ProvenanceNodeType, Some("Value"))
            case Relation(value)     => SqlRelationRenderTree(implicitly[RenderTree[Expr]]).render(value).copy(nodeType = ProvenanceNodeType)
            case Either(left, right) => nest(self.render(left), self.render(right), "|")
            case Both(left, right)   => nest(self.render(left), self.render(right), "&")
        }
      }
    }

    implicit val ProvenanceOrMonoid: Monoid[Provenance] =
      new Monoid[Provenance] {
        import Provenance._

        def zero = Empty

        def append(v1: Provenance, v2: => Provenance) = (v1, v2) match {
          case (Empty, that) => that
          case (this0, Empty) => this0
          case _ => v1 | v2
        }
      }

    implicit val ProvenanceAndMonoid: Monoid[Provenance] =
      new Monoid[Provenance] {
        import Provenance._

        def zero = Empty

        def append(v1: Provenance, v2: => Provenance) = (v1, v2) match {
          case (Empty, that) => that
          case (this0, Empty) => this0
          case _ => v1 & v2
        }
      }
  }
  object Provenance extends ProvenanceInstances {
    case object Empty extends Provenance
    case object Value extends Provenance
    case class Relation(value: SqlRelation[Expr]) extends Provenance
    case class Either(left: Provenance, right: Provenance) extends Provenance {
      override def flatten: Set[Provenance] = {
        def flatten0(x: Provenance): Set[Provenance] = x match {
          case Either(left, right) => flatten0(left) ++ flatten0(right)
          case _ => Set(x)
        }
        flatten0(this)
      }
    }
    case class Both(left: Provenance, right: Provenance) extends Provenance {
      override def flatten: Set[Provenance] = {
        def flatten0(x: Provenance): Set[Provenance] = x match {
          case Both(left, right) => flatten0(left) ++ flatten0(right)
          case _ => Set(x)
        }
        flatten0(this)
      }
    }

    def allOf[F[_]: Foldable](xs: F[Provenance]): Provenance =
      xs.concatenate(ProvenanceAndMonoid)

    def anyOf[F[_]: Foldable](xs: F[Provenance]): Provenance =
      xs.concatenate(ProvenanceOrMonoid)
  }

  /** This phase infers the provenance of every expression, issuing errors
    * if identifiers are used with unknown provenance. The phase requires
    * TableScope annotations on the tree.
    */
  val inferProvenanceƒ:
      (TableScope, ExprF[Provenance]) => ValidSem[Provenance] = (tableScope, expr) => expr match {
    case SelectF(_, projections, _, _, _, _) =>
      success(Provenance.allOf(projections.map(_.expr)))

    case SetLiteralF(_)  => success(Provenance.Value)
    case ArrayLiteralF(_) => success(Provenance.Value)
    case SpliceF(expr)       => success(expr.getOrElse(Provenance.Empty))
    case VariF(_)        => success(Provenance.Value)
    case BinopF(left, right, _) => success(left & right)
    case UnopF(expr, _) => success(expr)
    case IdentF(name) =>
      tableScope.scope.get(name).fold(
        Provenance.anyOf[Map[String, ?]](tableScope.scope ∘ (Provenance.Relation(_))) match {
          case Provenance.Empty => fail(NoTableDefined(Ident(name)))
          case x                => success(x)
        })(
        (Provenance.Relation(_)) ⋙ success)
    case InvokeFunctionF(_, args) => success(Provenance.allOf(args))
    case MatchF(_, cases, _)      =>
      success(cases.map(_.expr).concatenate(Provenance.ProvenanceAndMonoid))
    case SwitchF(cases, _)        =>
      success(cases.map(_.expr).concatenate(Provenance.ProvenanceAndMonoid))
    case IntLiteralF(_)           => success(Provenance.Value)
    case FloatLiteralF(_)         => success(Provenance.Value)
    case StringLiteralF(_)        => success(Provenance.Value)
    case BoolLiteralF(_)          => success(Provenance.Value)
    case NullLiteralF()           => success(Provenance.Value)
  }

  type Annotations = (List[Option[Synthetic]], Provenance)

  // NB: converts identifySyntheticsƒ from a cata to a coelgotM, for zipping
  val synthCoEƒ:
      (TableScope, ExprF[List[Option[Synthetic]]]) => ValidSem[List[Option[Synthetic]]] =
    generalizeCoelgot(identifySyntheticsƒ)(_, _).point[ValidSem]

  def projectSortKeys(expr: Expr) = expr.cata(projectSortKeysƒ)
  def scopeTables(a: (TableScope, Expr)) = (scopeTablesƒ).tupled(a).disjunction
  def addAnnotations(a: (TableScope, Expr), node: ExprF[Cofree[ExprF, Annotations]]) =
    attributeCoelgotM(
      zipCoelgotM(synthCoEƒ, inferProvenanceƒ)).apply(a._1, node).disjunction

  // NB: projectSortKeys >>> (identifySynthetics &&& (scopeTables >>> inferProvenance))
  def AllPhases(expr: Expr) =
    coelgotM[NonEmptyList[SemanticError] \/ ?](
      (TableScope(Map()), projectSortKeys(expr)))(
      addAnnotations, scopeTables)
}

object SemanticAnalysis extends SemanticAnalysis
