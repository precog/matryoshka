package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis._
import slamdata.engine.std.Library

import scalaz.{Tree => _, Node => _, _}
import Scalaz._

trait SemanticAnalysis {
  import slamdata.engine.sql._
  import SemanticError._

  type Failure = NonEmptyList[SemanticError]

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

  sealed trait Synthetic
  object Synthetic {
    case object SortKey extends Synthetic
  }

  implicit val SyntheticRenderTree = new RenderTree[Synthetic] {
    def render(v: Synthetic) = Terminal(v.toString, List("Synthetic"))
  }

  /**
   * Inserts synthetic fields into the projections of each `select` stmt to hold
   * the values that will be used in sorting, and annotates each new projection
   * with Synthetic.SortKey. The compiler will generate a step to remove these
   * fields after the sort operation.
   */
  def TransformSelect[A]: Analysis[Node, A, Option[Synthetic], Failure] = {
    val prefix = "__sd__"

    def transform(node: Node): Node =
      node match {
        case sel @ SelectStmt(_, projections, _, _, _, Some(sql.OrderBy(keys)), _, _) => {
          def matches(key: Expr, proj: Proj): Boolean = (key, proj) match {
            case (Ident(keyName), Proj.Anon(Ident(projName))) => keyName == projName
            case (Ident(keyName), Proj.Anon(Splice(_)))       => true
            case (Ident(keyName), Proj.Named(_, alias))       => keyName == alias
            case _                                            => false
          }

          // Note: order of the keys has to be preserved, so this complex fold seems
          // to be the best way.
          type Target = (List[Proj], List[(Expr, OrderType)], Int)

          val (projs2, keys2, _) = keys.foldRight[Target]((Nil, Nil, 0)) {
            case (key @ (expr, orderType), (projs, keys, index)) =>
              if (!projections.exists(matches(expr, _))) {
                val name  = prefix + index.toString()
                val proj2 = Proj.Named(expr, name)
                val key2  = Ident(name) -> orderType

                (proj2 :: projs, key2 :: keys, index + 1)
              } else (projs, key :: keys, index)
          }

          sel.copy(projections = projections ++ projs2,
                   orderBy     = Some(sql.OrderBy(keys2)))
        }

        case _ => node
      }

    val ann = Analysis.annotate[Node, Unit, Option[Synthetic], Failure] { node =>
      node match {
        case Proj.Named(_, name) if name.startsWith(prefix) =>
          Some(Synthetic.SortKey).success

        case _ => None.success
      }
    }

    tree1 => ann(tree(transform(tree1.root)))
  }


  case class TableScope(scope: Map[String, SqlRelation])

  implicit val ShowTableScope = new Show[TableScope] {
    override def show(v: TableScope) = Show[Map[String, Node]].show(v.scope)
  }

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
        case SelectStmt(_, projections, relations, filter, groupBy, orderBy, limit, offset) =>
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
                        case TableRelationAST(name, aliasOpt) => Some(aliasOpt.getOrElse(name))
                        case SubqueryRelationAST(subquery, alias) => Some(alias)
                        case JoinRelation(left, right, join, clause) => None
                        case CrossRelation(left, right) => None
                      }

                      (name.map { name =>
                        (acc.get(name).map{ relation2 =>
                          fail(DuplicateRelationName(name, relation2))
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
    import Provenance._

    def & (that: Provenance): Provenance = Both(this, that)

    def | (that: Provenance): Provenance = Either(this, that)

    def simplify: Provenance = this match {
      case x : Either => anyOf(x.flatten.map(_.simplify).filterNot(_ == Empty))
      case x : Both => allOf(x.flatten.map(_.simplify).filterNot(_ == Empty))
      case _ => this
    }

    def namedRelations: Map[String, List[NamedRelation]] = Foldable[List].foldMap(relations)(_.namedRelations)

    def relations: List[SqlRelation] = this match {
      case Empty => Nil
      case Value => Nil
      case Relation(value) => value :: Nil
      case Either(v1, v2) => v1.relations ++ v2.relations
      case Both(v1, v2) => v1.relations ++ v2.relations
    }

    def flatten: Set[Provenance] = Set(this)

    override def equals(that: Any): Boolean = (this, that) match {
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
    implicit val ProvenanceRenderTree = new RenderTree[Provenance] { self =>
      import Provenance._

      override def render(v: Provenance) = {
        val ProvenanceNodeType = List("Provenance")

        def nest(l: RenderedTree, r: RenderedTree, sep: String) = (l, r) match {
          case (RenderedTree(ll, Nil, lt), RenderedTree(rl, Nil, rt)) =>
                    Terminal("(" + ll + " " + sep + " " + rl + ")", ProvenanceNodeType)
          case _ => NonTerminal(sep, l :: r :: Nil, ProvenanceNodeType)
        }

        v match {
          case Empty               => Terminal("Empty", ProvenanceNodeType)
          case Value               => Terminal("Value", ProvenanceNodeType)
          case Relation(value)     => RenderTree[Node].render(value).copy(nodeType=ProvenanceNodeType)
          case Either(left, right) => nest(self.render(left), self.render(right), "|")
          case Both(left, right)   => nest(self.render(left), self.render(right), "&")
        }
      }
    }
  }
  object Provenance extends ProvenanceInstances {
    case object Empty extends Provenance
    case object Value extends Provenance
    case class Relation(value: SqlRelation) extends Provenance
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

    def allOf(xs: Iterable[Provenance]): Provenance = {
      if (xs.size == 0) Empty
      else if (xs.size == 1) xs.head
      else xs.reduce(_ & _)
    }

    def anyOf(xs: Iterable[Provenance]): Provenance = {
      if (xs.size == 0) Empty
      else if (xs.size == 1) xs.head
      else xs.reduce(_ | _)
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

      def propagate(child: Node) = success(provOf(child))

      def NA: Validation[Nothing, Provenance] = success(Provenance.Empty)

      (node match {
        case SelectStmt(_, projections, relations, filter, groupBy, orderBy, limit, offset) =>
          success(Provenance.allOf(projections.map(provOf)))

        case Proj.Anon(expr)    => propagate(expr)
        case Proj.Named(expr, _) => propagate(expr)
        case Subselect(select)  => propagate(select)
        case SetLiteral(values) => success(Provenance.Value)
          // FIXME: NA case
        case Splice(expr)       => expr.fold(NA)(x => success(provOf(x)))
        case v @ Vari(_)        => success(Provenance.Value)
        case Binop(left, right, op) =>
          success(provOf(left) & provOf(right))
        case Unop(expr, op) => success(provOf(expr))
        case ident @ Ident(name) =>
          val tableScope = tree.attr(node).scope

          (tableScope.get(name).map((Provenance.Relation.apply _) andThen success)).getOrElse {
            Provenance.anyOf(tableScope.values.map(Provenance.Relation.apply)) match {
              case Provenance.Empty => fail(NoTableDefined(ident))

              case x => success(x)
            }
          }

        case InvokeFunction(name, args) =>
          success(Provenance.allOf(args.map(provOf)))
        case Case(cond, expr) => propagate(expr)
        case Match(expr, cases, default) =>
          success(cases.map(provOf).reduce(_ & _))
        case Switch(cases, default) => success(cases.map(provOf).reduce(_ & _))

        case IntLiteral(value) => success(Provenance.Value)

        case FloatLiteral(value) => success(Provenance.Value)

        case StringLiteral(value) => success(Provenance.Value)

        case BoolLiteral(value) => success(Provenance.Value)

        case NullLiteral() => success(Provenance.Value)

        case r @ TableRelationAST(name, alias) => success(Provenance.Relation(r))

        case r @ SubqueryRelationAST(subquery, alias) => success(Provenance.Relation(r))

        case r @ JoinRelation(left, right, tpe, clause) => success(Provenance.Relation(r))

        case r @ CrossRelation(left, right) => success(Provenance.Relation(r))

        case GroupBy(keys, having) => success(Provenance.allOf(keys.map(provOf)))

        case OrderBy(keys) => success(Provenance.allOf(keys.map(_._1).toList.map(provOf)))

        case _ : BinaryOperator => NA

        case _ : UnaryOperator => NA
      }).map(_.simplify)
    })
  }

  sealed trait InferredType
  object InferredType {
    case class Specific(value: Type) extends InferredType
    case object Unknown extends InferredType

    implicit val ShowInferredType = new Show[InferredType] {
      override def show(v: InferredType) = v match {
        case Unknown => Cord("?")
        case Specific(v) => Show[Type].show(v)
      }
    }
  }

  /**
   * This phase works top-down to push out known types to terms with unknowable
   * types (such as columns and wildcards). The annotation is the type of the node,
   * which defaults to Type.Top in cases where it is not known.
   */
  def TypeInfer = {
    Analysis.readTree[Node, Option[Func], Map[Node, Type], Failure] { tree =>
      import Validation.{success}

      Analysis.fork[Node, Option[Func], Map[Node, Type], Failure]((mapOf, node) => {
        /**
         * Retrieves the inferred type of the current node being annotated.
         */
        def inferredType = for {
          parent   <- tree.parent(node)
          selfType <- mapOf(parent).get(node)
        } yield selfType

        /**
         * Propagates the inferred type of this node to its sole child node.
         */
        def propagate(child: Node) = propagateAll(child :: Nil)

        /**
         * Propagates the inferred type of this node to its identically-typed
         * children nodes.
         */
        def propagateAll(children: Seq[Node]) = success(inferredType.map(t => Map(children.map(_ -> t): _*)).getOrElse(Map()))

        def annotateFunction(args: List[Node]) =
          (tree.attr(node).map { func =>
            val typesV = inferredType.map(func.unapply).getOrElse(success(func.domain))
            typesV map (types => (args zip types).toMap)
          }).getOrElse(fail(FunctionNotBound(node)))

        /**
         * Indicates no information content for the children of this node.
         */
        def NA = success(Map.empty[Node, Type])

        node match {
          case SelectStmt(_, projections, relations, filter, groupBy, orderBy, limit, offset) =>
            inferredType match {
              // TODO: If there's enough type information in the inferred type to do so, push it
              //       down to the projections.

              case _ => NA
            }

          case Proj.Anon(expr) => propagate(expr)
          case Proj.Named(expr, _) => propagate(expr)
          case Subselect(select) => propagate(select)
          case SetLiteral(values) =>
            inferredType match {
              // Push the set type down to the children:
              case Some(Type.Set(tpe)) => success(values.map(_ -> tpe).toMap)

              case _ => NA
            }
          case Splice(expr) => expr.fold(NA)(propagate(_))
          case v @ Vari(_) => NA
          case Binop(left, right, _) => annotateFunction(left :: right :: Nil)
          case Unop(expr, _) => annotateFunction(expr :: Nil)
          case Ident(name) => NA
          case InvokeFunction(_, args) => annotateFunction(args)
          case Case(cond, expr) => propagate(expr)
          case Match(expr, cases, default) => propagateAll(cases ++ default)
          case Switch(cases, default) => propagateAll(cases ++ default)
          case IntLiteral(_) => NA
          case FloatLiteral(_) => NA
          case StringLiteral(_) => NA
          case BoolLiteral(_) => NA
          case NullLiteral() => NA
          case TableRelationAST(name, alias) => NA
          case SubqueryRelationAST(subquery, alias) => propagate(subquery)
          case JoinRelation(left, right, tpe, clause) => NA
          case CrossRelation(left, right) => NA
          case GroupBy(keys, having) => NA
          case OrderBy(keys) => NA
          case _: BinaryOperator => NA
          case _: UnaryOperator  => NA
        }
      })
    } >>> Analysis.readTree[Node, Map[Node, Type], InferredType, Failure] { tree =>
      Analysis.fork[Node, Map[Node, Type], InferredType, Failure]((typeOf, node) => {
        // Read the inferred type of this node from the parent node's attribute:
        succeed((for {
          parent   <- tree.parent(node)
          selfType <- tree.attr(parent).get(node)
        } yield selfType).map(InferredType.Specific.apply).getOrElse(InferredType.Unknown))
      })
    }
  }

  /**
   * This phase works bottom-up to check the type of all expressions.
   * In the event of a type error, an error will be produced containing
   * details on the expected versus actual type.
   */
  def TypeCheck = {
    Analysis.readTree[Node, (Option[Func], InferredType), Type, Failure] { tree =>
      Analysis.join[Node, (Option[Func], InferredType), Type, Failure]((typeOf, node) => {
        def func(node: Node): ValidationNel[SemanticError, Func] = {
          tree.attr(node)._1.map(Validation.success).getOrElse(fail(FunctionNotBound(node)))
        }

        def inferType(default: Type): ValidationNel[SemanticError, Type] = succeed(tree.attr(node)._2 match {
          case InferredType.Unknown => default
          case InferredType.Specific(v) => v
        })

        def typecheckArgs(func: Func, actual: List[Type]): ValidationNel[SemanticError, Unit] = {
          val expected = func.domain

          if (expected.length != actual.length) {
            fail[Unit](WrongArgumentCount(func, expected.length, actual.length))
          } else {
            (expected.zip(actual).map {
              case (expected, actual) => Type.typecheck(expected, actual)
            }).sequenceU.map(κ(Unit))
          }
        }

        def typecheckFunc(args: List[Expr]) = {
          func(node).fold(
            Validation.failure,
            func => {
              val argTypes = args.map(typeOf)

              typecheckArgs(func, argTypes).fold(
                Validation.failure,
                κ(func.apply(argTypes))
              )
            }
          )
        }

        def NA = succeed(Type.Bottom)

        def propagate(n: Node) = succeed(typeOf(n))

        node match {
          case s @ SelectStmt(_, projections, relations, filter, groupBy, orderBy, limit, offset) =>
            succeed(Type.makeObject(s.namedProjections(None).map(t => (t._1, typeOf(t._2)))))

          case Proj.Anon(expr) => propagate(expr)
          case Proj.Named(expr, _) => propagate(expr)
          case Subselect(select) => propagate(select)
          case SetLiteral(values) => succeed(Type.makeArray(values.map(typeOf)))
          case Splice(_) => inferType(Type.Top)
          case v @ Vari(_) => inferType(Type.Top)
          case Binop(left, right, op) => typecheckFunc(left :: right :: Nil)
          case Unop(expr, op) => typecheckFunc(expr :: Nil)
          case Ident(name) => inferType(Type.Top)
          case InvokeFunction(name, args) => typecheckFunc(args.toList)
          case Case(cond, expr) => succeed(typeOf(expr))
          case Match(expr, cases, default) =>
            succeed((cases ++ default).map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)
          case Switch(cases, default) =>
            succeed((cases ++ default).map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)
          case IntLiteral(value) => succeed(Type.Const(Data.Int(value)))
          case FloatLiteral(value) => succeed(Type.Const(Data.Dec(value)))
          case StringLiteral(value) => succeed(Type.Const(Data.Str(value)))
          case BoolLiteral(value) => succeed(Type.Const(Data.Bool(value)))
          case NullLiteral() => succeed(Type.Const(Data.Null))
          case TableRelationAST(name, alias) => NA
          case SubqueryRelationAST(subquery, alias) => propagate(subquery)
          case JoinRelation(left, right, tpe, clause) => succeed(Type.Bool)
          case CrossRelation(left, right) => succeed(typeOf(left) & typeOf(right))
          case GroupBy(keys, having) =>
            // Not necessary but might be useful:
            succeed(Type.makeArray(keys.map(typeOf)))
          case OrderBy(keys) => NA
          case _: BinaryOperator => NA
          case _: UnaryOperator  => NA
        }
      })
    }
  }

  type Annotations = (((Option[Synthetic], Provenance), Option[Func]), Type)

  val AllPhases: Analysis[Node, Unit, Annotations, Failure] =
    (TransformSelect[Unit].push(()) >>>
      ScopeTables.second >>>
      ProvenanceInfer.second).push(()) >>>
    FunctionBind[Unit](std.StdLib).second.dup2 >>>
    TypeInfer.second >>>
    TypeCheck.pop2
}
object SemanticAnalysis extends SemanticAnalysis
