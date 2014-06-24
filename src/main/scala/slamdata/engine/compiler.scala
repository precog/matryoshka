package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.sql._
import slamdata.engine.fs.Path
import slamdata.engine.analysis.fixplate._

import SemanticAnalysis._
import SemanticError._
import slamdata.engine.std.StdLib._

import scalaz.{Id, Free, Monad, EitherT, StateT, IndexedStateT, Applicative, \/, Foldable}

import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._

trait Compiler[F[_]] {
  import set._
  import relations._
  import structural._
  import math._
  import agg._

  import Compiler.Ann

  private def typeOf(node: Node)(implicit m: Monad[F]): StateT[M, CompilerState, Type] = attr(node).map(_._1._1)

  private def provenanceOf(node: Node)(implicit m: Monad[F]): StateT[M, CompilerState, Provenance] = attr(node).map(_._2)

  private def funcOf(node: Node)(implicit m: Monad[F]): StateT[M, CompilerState, Func] = for {
    funcOpt <- attr(node).map(_._1._2)
    rez     <- funcOpt.map(emit _).getOrElse(fail(FunctionNotBound(node)))
  } yield rez

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerM[A] = StateT[M, CompilerState, A]

  private case class TableContext(
    root: Option[Term[LogicalPlan]],
    full: () => Term [LogicalPlan],
    subtables: Map[String, Term[LogicalPlan]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => LogicalPlan.invoke(ObjectConcat, List(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private case class CompilerState(
    tree:         AnnotatedTree[Node, Ann], 
    tableContext: List[TableContext] = Nil,
    nameGen:      Int = 0
  )

  private object CompilerState {
    /**
     * Runs a computation inside a table context, which contains compilation
     * data for the tables in scope.
     */
    def contextual[A](t: TableContext)(f: CompilerM[A])(implicit m: Monad[F]): CompilerM[A] = for {
      _ <- mod((s: CompilerState) => s.copy(tableContext = t :: s.tableContext))
      a <- f
      _ <- mod((s: CompilerState) => s.copy(tableContext = s.tableContext.tail))
    } yield a

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] =
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit m: Monad[F]): CompilerM[Term[LogicalPlan]] = {
      this.rootTable flatMap {
        case Some(t)  => emit(t)
        case None     => fail(CompiledTableMissing)
      }
    }

    def subtable(name: String)(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] = 
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.flatMap(_.subtables.get(name)))

    def subtableReq(name: String)(implicit m: Monad[F]): CompilerM[Term[LogicalPlan]] = {
      subtable(name) flatMap {
        case Some(t) => emit(t)
        case None    => fail(CompiledSubtableMissing(name))
      }
    }

    def fullTable(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] =
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.map(_.full()))

    def fullTableReq(implicit m: Monad[F]): CompilerM[Term[LogicalPlan]] =
      fullTable flatMap {
        case Some(t) => emit(t)
        case None    => fail(CompiledTableMissing)
      }

    /**
     * Generates a fresh name for use as an identifier, e.g. tmp321.
     */
    def freshName(prefix: String)(implicit m: Monad[F]): CompilerM[Symbol] =
      for {
        num <- read[CompilerState, Int](_.nameGen)
        _   <- mod((s: CompilerState) => s.copy(nameGen = s.nameGen + 1))
      } yield Symbol(prefix + num.toString)
  }

  sealed trait JoinTraverse {
    import JoinTraverse._

    def directionMap: Map[String, List[Dir]] = {
      def loop(v: JoinTraverse, acc: List[Dir]): Map[String, List[Dir]] = v match {
        case Leaf(name) => Map(name -> acc)

        case Join(left, right) =>
          loop(left, Left :: acc) ++
          loop(right, Right :: acc)
      }

      loop(this, Nil)
    }
  }

  object JoinTraverse {
    case class Leaf(name: String) extends JoinTraverse
    case class Join(left: JoinTraverse, right: JoinTraverse) extends JoinTraverse

    sealed trait Dir
    case object Left extends Dir
    case object Right extends Dir
  }

  private def read[A, B](f: A => B)(implicit m: Monad[F]): StateT[M, A, B] = StateT((s: A) => Applicative[M].point((s, f(s))))

  private def attr(node: Node)(implicit m: Monad[F]): StateT[M, CompilerState, Ann] = read(s => s.tree.attr(node))

  private def tree(implicit m: Monad[F]): StateT[M, CompilerState, AnnotatedTree[Node, Ann]] = read(s => s.tree)

  private def fail[A](error: SemanticError)(implicit m: Monad[F]): StateT[M, CompilerState, A] = {
    StateT[M, CompilerState, A]((s: CompilerState) => EitherT.eitherT(Applicative[F].point(\/.left(error))))
  }

  private def emit[A](value: A)(implicit m: Monad[F]): StateT[M, CompilerState, A] = {
    StateT[M, CompilerState, A]((s: CompilerState) => EitherT.eitherT(Applicative[F].point(\/.right(s -> value))))
  }

  private def whatif[S, A](f: StateT[M, S, A])(implicit m: Monad[F]): StateT[M, S, A] = {
    for {
      oldState <- read(identity[S])
      rez      <- f.imap(Function.const(oldState))
    } yield rez
  }

  private def mod(f: CompilerState => CompilerState)(implicit m: Monad[F]): StateT[M, CompilerState, Unit] = 
    StateT[M, CompilerState, Unit](s => Applicative[M].point(f(s) -> Unit))

  // TODO: Make this a desugaring pass once AST transformations are supported
  // Note: these transformations are applied _after_ the arguments are compiled
  def specialized1(func: Func, args: List[Term[LogicalPlan]]): Term[LogicalPlan] = (func, args) match {
    case (`Negate`, args)              => Multiply((LogicalPlan.constant(Data.Int(-1)) :: args): _*)
    case (`Range`, min :: max :: Nil)  => ArrayConcat(MakeArray(min), MakeArray(max))

    case (func, args)                  => func.apply(args: _*)
  }

  private def invoke(func: Func, args: List[Node])(implicit m: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan]] = {
    for {
      args <- args.map(compile0).sequenceU
      node = specialized1(func, args)
    } yield node
  }

  def transformOrderBy(select: SelectStmt): SelectStmt = {
    (select.orderBy.map { orderBy =>
      ???
    }).getOrElse(select)
  }

  // CORE COMPILER
  private def compile0(node: Node)(implicit M: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan]] = {
    def optInvoke2[A <: Node](default: Term[LogicalPlan], option: Option[A])(func: Func) = {
      option.map(compile0).map(_.map(c => LogicalPlan.invoke(func, default :: c :: Nil))).getOrElse(emit(default))
    }

    def compileCases(cases: List[Case], default: Node)(f: Case => CompilerM[(Term[LogicalPlan], Term[LogicalPlan])]) = {
     for {
        cases   <- cases.map(f).sequenceU
        default <- compile0(default)
      } yield cases.foldRight(default) { case ((cond, expr), default) => 
        LogicalPlan.invoke(relations.Cond, cond :: expr :: default :: Nil) 
      }
    }

    def flattenJoins(term: Term[LogicalPlan], relations: SqlRelation):
        Term[LogicalPlan] = relations match {
      case _: TableRelationAST => term
      case _: SubqueryRelationAST => term
      case JoinRelation(left, right, _, _) =>
        LogicalPlan.invoke(ObjectConcat,
          List(
            flattenJoins(LogicalPlan.invoke(ObjectProject, List(term, LogicalPlan.constant(Data.Str("left")))), left),
            flattenJoins(LogicalPlan.invoke(ObjectProject, List(term, LogicalPlan.constant(Data.Str("right")))), right)))
      case CrossRelation(left, right) =>
        LogicalPlan.invoke(ObjectConcat,
          List(
            flattenJoins(LogicalPlan.invoke(ObjectProject, List(term, LogicalPlan.constant(Data.Str("left")))), left),
            flattenJoins(LogicalPlan.invoke(ObjectProject, List(term, LogicalPlan.constant(Data.Str("right")))), right)))
    }

    def buildJoinDirectionMap(relations: SqlRelation): Map[String, List[JoinTraverse.Dir]] = {
      def loop(rel: SqlRelation): JoinTraverse = rel match {
        case t @ TableRelationAST(name, aliasOpt) => JoinTraverse.Leaf(t.aliasName)
        case t @ SubqueryRelationAST(subquery, alias) => JoinTraverse.Leaf(t.aliasName) // Leaf????
        case JoinRelation(left, right, tpe, clause) => JoinTraverse.Join(loop(left), loop(right))
        case CrossRelation(left, right) => JoinTraverse.Join(loop(left), loop(right))
      }

      loop(relations).directionMap
    }

    def compileTableRefs(joined: Term[LogicalPlan], relations: SqlRelation): Map[String, Term[LogicalPlan]] = {
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(joined) {
            case (dir, acc) =>
              val dirName = if (dir == JoinTraverse.Left) "left" else "right"

              LogicalPlan.invoke(ObjectProject, acc :: LogicalPlan.constant(Data.Str(dirName)) :: Nil)
          }
      }
    }

    def tableContext(joined: Term[LogicalPlan], relations: SqlRelation): TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation): (Option[CompilerM[Term[LogicalPlan]]] => CompilerM[Term[LogicalPlan]] => CompilerM[Term[LogicalPlan]]) = { 
        (current: Option[CompilerM[Term[LogicalPlan]]]) => (next: CompilerM[Term[LogicalPlan]]) => 
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          next2    <- CompilerState.contextual(tableContext(LogicalPlan.free(stepName), relations))(next)
        } yield LogicalPlan.let(Map(stepName -> current), next2)
      }.getOrElse(next)
    }

    def find1Ident(expr: Expr): CompilerM[Ident] = {
      val tree = Tree[Node](expr, _.children)

      (tree.collect {
        case x @ Ident(_) => x
      }) match {
        case one :: Nil => emit(one)
        case _ => fail(ExpectedOneTableInJoin(expr))
      }
    }

    def relationName(node: Node): CompilerM[String] = {
      for {
        prov <- provenanceOf(node)

        relations = prov.namedRelations

        name <- relations.headOption match {
                  case None => fail(NoTableDefined(node))
                  case Some((name, _)) if (relations.size == 1) => emit(name)
                  case _ => fail(AmbiguousReference(node, prov.relations))
                }
      } yield name
    }

    def compileJoin(clause: Expr): StateT[M, CompilerState, (LogicalPlan.JoinRel, Term[LogicalPlan], Term[LogicalPlan])] = {
      clause match {
        case InvokeFunction(f, left :: right :: Nil) =>
          val joinRel = 
            if (f == relations.Eq) emit(LogicalPlan.JoinRel.Eq)
            else if (f == relations.Lt) emit(LogicalPlan.JoinRel.Lt)
            else if (f == relations.Gt) emit(LogicalPlan.JoinRel.Gt)
            else if (f == relations.Lte) emit(LogicalPlan.JoinRel.Lte)
            else if (f == relations.Gte) emit(LogicalPlan.JoinRel.Gte)
            else if (f == relations.Neq) emit(LogicalPlan.JoinRel.Neq)
            else fail(UnsupportedJoinCondition(clause))

          for {
            rel   <- joinRel
            left  <- compile0(left)
            right <- compile0(right)
            rez   <- emit((rel, left, right))
          } yield rez

        case Binop (left, right, op) =>
          val joinRel = op match {
            case sql.Eq  => emit(LogicalPlan.JoinRel.Eq)
            case sql.Lt  => emit(LogicalPlan.JoinRel.Lt)
            case sql.Gt  => emit(LogicalPlan.JoinRel.Gt)
            case sql.Le  => emit(LogicalPlan.JoinRel.Lte)
            case sql.Ge  => emit(LogicalPlan.JoinRel.Gte)
            case sql.Neq => emit(LogicalPlan.JoinRel.Neq)
            case _ => fail(UnsupportedJoinCondition(clause))
          }
          for {
            rel   <- joinRel
            left  <- compile0(left)
            right <- compile0(right)
            rez   <- emit((rel, left, right))
          } yield rez

        case _ => fail(UnsupportedJoinCondition(clause))
      }
    }

    def compileFunction(func: Func, args: List[Expr]): StateT[M, CompilerState, Term[LogicalPlan]] = {
      // TODO: Make this a desugaring pass once AST transformations are supported
      // Note: these transformations are applied _before_ the arguments are compiled
      val specialized: PartialFunction[(Func, List[Expr]), StateT[M, CompilerState, Term[LogicalPlan]]] = {
        case (`ArrayProject`, arry :: Wildcard :: Nil) => compileFunction(structural.FlattenArray, arry :: Nil)
      }

      val default: (Func, List[Expr]) => StateT[M, CompilerState, Term[LogicalPlan]] = { (func, args) =>
        for {
          args <- args.map(compile0).sequenceU
          node = specialized1(func, args)  // Second attempt to transfrom, after compiling the args
        } yield node
      }

      specialized.applyOrElse((func, args), default.tupled)
    }

    def buildRecord(names: List[Option[String]], values: List[Term[LogicalPlan]]): Term[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) => LogicalPlan.invoke(MakeObject, LogicalPlan.constant(Data.Str(name)) :: value :: Nil)//: Term[LogicalPlan]
        case (None, value) => value
      }

      fields.reduce((a, b) => LogicalPlan.invoke(ObjectConcat, a :: b :: Nil))
    }

    def buildArray(values: List[Term[LogicalPlan]]): Term[LogicalPlan] = {
      val singletons = values.map((t: Term[LogicalPlan]) => MakeArray(t))
      singletons.reduce((t1: Term[LogicalPlan], t2: Term[LogicalPlan]) => ArrayConcat(t1, t2))
    }
      
    def compileArray[A <: Node](list: List[A]): CompilerM[Term[LogicalPlan]] = {
      for {
        list <- list.map(compile0 _).sequenceU
      } yield buildArray(list)
    }
    
    node match {
      case s @ SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
        /* 
         * 1. Joins, crosses, subselects (FROM)
         * 2. Filter (WHERE)
         * 3. Group by (GROUP BY)
         * 4. Filter (HAVING)
         * 5. Select (SELECT)
         * 6. Sort (ORDER BY)
         * 7. Drop (OFFSET)
         * 8. Take (LIMIT)
         *
         */

        // Selection of wildcards aren't named, we merge them into any other objects created from other columns:
        val names = s.namedProjections.map {
          case (name, Wildcard) => None
          case (name, value)    => Some(name)
        }

        val projs = projections.map(_.expr)

        relations match {
          case None => for {
            projs <- projs.map(compile0).sequenceU
          } yield buildRecord(names, projs)
          case Some(relations) => {
            val stepBuilder = step(relations)
            stepBuilder(Some(compile0(relations))) {
              val filtered = filter map { filter =>
                for {
                  t <- CompilerState.rootTableReq
                  f <- compile0(filter)
                } yield Filter(t, f)
              }

              stepBuilder(filtered) {
                val grouped = groupBy map { groupBy =>
                  for {
                    t <- CompilerState.rootTableReq
                    g <- compileArray(groupBy.keys)
                  } yield GroupBy(t, g)
                }

                stepBuilder(grouped) {
                  val having = groupBy.flatMap(_.having) map { having =>
                    for {
                      t <- CompilerState.rootTableReq
                      h <- compile0(having)
                    } yield Filter(t, h)
                  }

                  stepBuilder(having) {
                    val select = Some {
                      for {
                        projs <- projs.map(compile0).sequenceU
                    } yield buildRecord(names, projs)
                    }

                    stepBuilder(select) {
                    def compileOrderByKey(key: Expr, ot: OrderType): CompilerM[Term[LogicalPlan]] =
                      for {
                        key <- compile0(key)
                      } yield buildRecord(Some("key") :: Some("order") :: Nil,
                                          key :: LogicalPlan.constant(Data.Str(ot.toString)) :: Nil)

                      val sort = orderBy map { orderBy =>
                        for {
                          t <- CompilerState.rootTableReq
                        keys <- orderBy.keys.map(t => compileOrderByKey(t._1, t._2)).sequenceU
                      } yield OrderBy(t, buildArray(keys))
                      }

                      stepBuilder(sort) {
                        // Note: inspecting the name is not the most awesome imaginable way to identify
                      // fields that were introduced to support "order by"
                        val synthetic: Option[String] => Boolean = {
                          case Some(name) => name.startsWith("__sd__")
                          case None => false
                        }
                        
                        val pruned = if (names.exists(synthetic))
                          Some {
                            for {
                              t <- CompilerState.rootTableReq
                              ns = names.collect {
                                case Some(name) if !synthetic(Some(name)) => name
                              }
                              ts = ns.map(name => ObjectProject(t, LogicalPlan.constant(Data.Str(name))))
                          } yield buildRecord(ns.map(name => Some(name)), ts)
                          }
                          else None
                        
                        stepBuilder(pruned) {
                          val drop = offset map { offset =>
                            for {
                              t <- CompilerState.rootTableReq
                            } yield Drop(t, LogicalPlan.constant(Data.Int(offset)))
                          }

                          stepBuilder(drop) {
                            (limit map { limit =>
                              for {
                                t <- CompilerState.rootTableReq
                              } yield Take(t, LogicalPlan.constant(Data.Int(limit)))
                            }).getOrElse(CompilerState.rootTableReq)
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }

      case Subselect(select) => compile0(select)

      case SetLiteral(values0) => 
        val values = (values0.map { 
          case IntLiteral(v) => emit[Data](Data.Int(v))
          case FloatLiteral(v) => emit[Data](Data.Dec(v))
          case StringLiteral(v) => emit[Data](Data.Str(v))
          case x => fail[Data](ExpectedLiteral(x))
        }).sequenceU

        values.map((Data.Set.apply _) andThen (LogicalPlan.constant _))

      case Wildcard =>
        // Except when it appears as the argument to ARRAY_PROJECT, wildcard
        // always means read everything from the fully joined.
        for {
          tableOpt <- CompilerState.fullTable
          table    <- tableOpt.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))
        } yield table

      case Binop(left, right, op) => 
        for {
          func  <- funcOf(node)
          rez   <- invoke(func, left :: right :: Nil)
        } yield rez

      case Unop(expr, op) => 
        for {
          func <- funcOf(node)
          rez  <- invoke(func, expr :: Nil)
        } yield rez

      case ident @ Ident(_) => 
        for {
          prov      <-  provenanceOf(node)
          name      <-  relationName(ident)
          table     <-  CompilerState.subtableReq(name)
          plan      <-  if (ident.name == name) emit(table) // Identifier is name of table, so just emit table plan
                        else emit(LogicalPlan.invoke(ObjectProject, table :: LogicalPlan.constant(Data.Str(ident.name)) :: Nil)) // Identifier is field
        } yield plan

      case InvokeFunction(name, args) => 
        for {
          func <- funcOf(node)
          rez  <- compileFunction(func, args)
        } yield rez

      case Match(expr, cases, default0) => 
        val default = default0.getOrElse(NullLiteral())
        
        for {
          expr  <-  compile0(expr)
          cases <-  compileCases(cases, default) {
                      case Case(cse, expr2) => 
                        for { 
                          cse   <- compile0(cse)
                          expr2 <- compile0(expr2)
                        } yield (LogicalPlan.invoke(relations.Eq, expr :: cse :: Nil), expr2) 
                    }
        } yield cases

      case Switch(cases, default0) => 
        val default = default0.getOrElse(NullLiteral())
        
        for {
          cases <-  compileCases(cases, default) { 
                      case Case(cond, expr2) => 
                        for { 
                          cond  <- compile0(cond)
                          expr2 <- compile0(expr2)
                        } yield (cond, expr2) 
                    }
        } yield cases

      case IntLiteral(value) => emit(LogicalPlan.constant(Data.Int(value)))

      case FloatLiteral(value) => emit(LogicalPlan.constant(Data.Dec(value)))

      case StringLiteral(value) => emit(LogicalPlan.constant(Data.Str(value)))

      case NullLiteral() => emit(LogicalPlan.constant(Data.Null))

      case TableRelationAST(name, _) => emit(LogicalPlan.read(Path(name)))

      case SubqueryRelationAST(subquery, _) => compile0(subquery)

      case JoinRelation(left, right, tpe, clause) => 
        for {
          leftName <- CompilerState.freshName("left")
          rightName <- CompilerState.freshName("right")
          leftFree = LogicalPlan.free(leftName)
          rightFree = LogicalPlan.free(rightName)
          left0 <- compile0(left)
          right0 <- compile0(right)
          join <- CompilerState.contextual(
            tableContext(leftFree, left) ++ tableContext(rightFree, right)
          ) {
            for {
              tuple  <- compileJoin(clause)
            } yield LogicalPlan.join(leftFree, rightFree,
              tpe match {
                case LeftJoin  => LogicalPlan.JoinType.LeftOuter
                case InnerJoin => LogicalPlan.JoinType.Inner
                case RightJoin => LogicalPlan.JoinType.RightOuter
                case FullJoin  => LogicalPlan.JoinType.FullOuter
              }, tuple._1, tuple._2, tuple._3)
          }
        } yield LogicalPlan.let(Map(leftName -> left0, rightName -> right0), join)

      case CrossRelation(left, right) =>
        for {
          left  <- compile0(left)
          right <- compile0(right)
        } yield Cross(left, right)

      case _ => fail(NonCompilableNode(node))
    }
  }

  def compile(tree: AnnotatedTree[Node, Ann])(implicit F: Monad[F]): F[SemanticError \/ Term[LogicalPlan]] = {
    compile0(tree.root).eval(CompilerState(tree)).run
  }
}

object Compiler {
  type Ann = ((Type, Option[Func]), Provenance)

  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def id = apply[Id.Id]

  def trampoline = apply[Free.Trampoline]

  def compile(tree: AnnotatedTree[Node, Ann]): SemanticError \/ Term[LogicalPlan] = {
    trampoline.compile(tree).run
  }
}
