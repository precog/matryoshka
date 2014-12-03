package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.sql._
import slamdata.engine.fs.Path
import slamdata.engine.analysis.fixplate._

import SemanticAnalysis._
import SemanticError._
import slamdata.engine.std.StdLib._

import scalaz.{Tree => _, Node => _, _}
import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._

trait Compiler[F[_]] {
  import set._
  import relations._
  import string._
  import structural._
  import math._
  import agg._

  import SemanticAnalysis.Annotations

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerM[A] = StateT[M, CompilerState, A]

  private def typeOf(node: Node)(implicit m: Monad[F]): CompilerM[Type] = attr(node).map(_._2)

  private def provenanceOf(node: Node)(implicit m: Monad[F]): CompilerM[Provenance] = attr(node).map(_._1._1._2)

  private def syntheticOf(node: Node)(implicit m: Monad[F]): CompilerM[Option[Synthetic]] = attr(node).map(_._1._1._1)

  private def funcOf(node: Node)(implicit m: Monad[F]): CompilerM[Func] = for {
    funcOpt <- attr(node).map(_._1._2)
    rez     <- funcOpt.map(emit _).getOrElse(fail(FunctionNotBound(node)))
  } yield rez

  private case class TableContext(
    root: Option[Term[LogicalPlan]],
    full: () => Term [LogicalPlan],
    subtables: Map[String, Term[LogicalPlan]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => LogicalPlan.Invoke(ObjectConcat, List(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private case class CompilerState(
    tree:         AnnotatedTree[Node, Annotations], 
    tableContext: List[TableContext] = Nil,
    nameGen:      Int = 0
  )

  private object CompilerState {
    /**
     * Runs a computation inside a table context, which contains compilation
     * data for the tables in scope.
     */
    def contextual[A](t: TableContext)(f: CompilerM[A])(implicit m: Monad[F]):
        CompilerM[A] = for {
      _ <- mod((s: CompilerState) => s.copy(tableContext = t :: s.tableContext))
      a <- f
      _ <- mod((s: CompilerState) => s.copy(tableContext = s.tableContext.drop(1)))
    } yield a

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] =
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit m: Monad[F]): CompilerM[Term[LogicalPlan]] = {
      this.rootTable flatMap {
        case Some(t)  => emit(t)
        case None     => fail(CompiledTableMissing)
      }
    }

    def subtable(name: String)(implicit m: Monad[F]):
        CompilerM[Option[Term[LogicalPlan]]] =
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.flatMap(_.subtables.get(name)))

    def subtableReq(name: String)(implicit m: Monad[F]):
        CompilerM[Term[LogicalPlan]] =
      subtable(name) flatMap {
        case Some(t) => emit(t)
        case None    => fail(CompiledSubtableMissing(name))
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

  sealed trait JoinDir
  case object Left extends JoinDir {
    override def toString: String = "left"
  } 
  case object Right extends JoinDir {
    override def toString: String = "right"
  }

  private def read[A, B](f: A => B)(implicit m: Monad[F]): StateT[M, A, B] =
    StateT((s: A) => Applicative[M].point((s, f(s))))

  private def attr(node: Node)(implicit m: Monad[F]): CompilerM[Annotations] =
    read(s => s.tree.attr(node))

  private def tree(implicit m: Monad[F]): CompilerM[AnnotatedTree[Node, Annotations]] =
    read(s => s.tree)

  private def fail[A](error: SemanticError)(implicit m: Monad[F]):
      CompilerM[A] =
    StateT[M, CompilerState, A]((s: CompilerState) =>
      EitherT.eitherT(Applicative[F].point(\/.left(error))))

  private def emit[A](value: A)(implicit m: Monad[F]): CompilerM[A] =
    StateT[M, CompilerState, A]((s: CompilerState) =>
      EitherT.eitherT(Applicative[F].point(\/.right(s -> value))))

  private def whatif[S, A](f: StateT[M, S, A])(implicit m: Monad[F]):
      StateT[M, S, A] =
    for {
      oldState <- read(identity[S])
      rez      <- f.imap(Function.const(oldState))
    } yield rez

  private def mod(f: CompilerState => CompilerState)(implicit m: Monad[F]):
      CompilerM[Unit] =
    StateT[M, CompilerState, Unit](s => Applicative[M].point(f(s) -> Unit))

  private def invoke(func: Func, args: List[Node])(implicit m: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan]] = {
    for {
      args <- args.map(compile0).sequenceU
    } yield func.apply(args: _*)
  }

  def transformOrderBy(select: SelectStmt): SelectStmt = {
    (select.orderBy.map { orderBy =>
      ???
    }).getOrElse(select)
  }

  // CORE COMPILER
  private def compile0(node: Node)(implicit M: Monad[F]):
      CompilerM[Term[LogicalPlan]] = {
    def optInvoke2[A <: Node](default: Term[LogicalPlan], option: Option[A])(func: Func) = {
      option.map(compile0).map(_.map(c => LogicalPlan.Invoke(func, default :: c :: Nil))).getOrElse(emit(default))
    }

    def compileCases(cases: List[Case], default: Node)(f: Case => CompilerM[(Term[LogicalPlan], Term[LogicalPlan])]) =
      for {
        cases   <- cases.map(f).sequenceU
        default <- compile0(default)
      } yield cases.foldRight(default) {
        case ((cond, expr), default) =>
          LogicalPlan.Invoke(relations.Cond, cond :: expr :: default :: Nil)
      }

    def regexForLikePattern(pattern: String, escapeChar: Option[Char]):
        String = {
      def sansEscape(pat: List[Char]): List[Char] = pat match {
        case '_' :: t =>         '.' +: escape(t)
        case '%' :: t => ".*".toList ++ escape(t)
        case c :: t   =>
          if ("\\^$.|?*+()[{".contains(c))
            '\\' +: escape(t)
          else c +: escape(t)
        case Nil      => Nil
      }

      def escape(pat: List[Char]): List[Char] =
        escapeChar match {
          case None => sansEscape(pat)
          case Some(esc) =>
            pat match {
              // NB: We only handle the escape char when it’s before a special
              //     char, otherwise you run into weird behavior when the escape
              //     char _is_ a special char. Will change if someone can find
              //     an actual definition of SQL’s semantics.
              case `esc` :: '%' :: t => '%' +: escape(t)
              case `esc` :: '_' :: t => '_' +: escape(t)
              case l                 => sansEscape(l)
            }
        }
      "^" + escape(pattern.toList).mkString + "$"
    }

    def flattenJoins(term: Term[LogicalPlan], relations: SqlRelation):
        Term[LogicalPlan] = relations match {
      case _: NamedRelation => term
      case JoinRelation(left, right, _, _) =>
        LogicalPlan.Invoke(ObjectConcat,
          List(
            flattenJoins(LogicalPlan.Invoke(ObjectProject, List(term, LogicalPlan.Constant(Data.Str("left")))), left),
            flattenJoins(LogicalPlan.Invoke(ObjectProject, List(term, LogicalPlan.Constant(Data.Str("right")))), right)))
      case CrossRelation(left, right) =>
        LogicalPlan.Invoke(ObjectConcat,
          List(
            flattenJoins(LogicalPlan.Invoke(ObjectProject, List(term, LogicalPlan.Constant(Data.Str("left")))), left),
            flattenJoins(LogicalPlan.Invoke(ObjectProject, List(term, LogicalPlan.Constant(Data.Str("right")))), right)))
    }

    def buildJoinDirectionMap(relations: SqlRelation): Map[String, List[JoinDir]] = {
      def loop(rel: SqlRelation, acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation => Map(t.aliasName -> acc)
        case JoinRelation(left, right, tpe, clause) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
        case CrossRelation(left, right) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }
 
      loop(relations, Nil)
    }

    def compileTableRefs(joined: Term[LogicalPlan], relations: SqlRelation): Map[String, Term[LogicalPlan]] = {
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(joined) {
            case (dir, acc) =>
              LogicalPlan.Invoke(
                ObjectProject,
                acc :: LogicalPlan.Constant(Data.Str(dir.toString)) :: Nil)
          }
      }
    }

    def tableContext(joined: Term[LogicalPlan], relations: SqlRelation): TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation):
        (Option[CompilerM[Term[LogicalPlan]]] =>
          CompilerM[Term[LogicalPlan]] =>
          CompilerM[Term[LogicalPlan]]) = {
      (current: Option[CompilerM[Term[LogicalPlan]]]) =>
      (next: CompilerM[Term[LogicalPlan]]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          next2    <- CompilerState.contextual(tableContext(LogicalPlan.Free(stepName), relations))(next)
        } yield LogicalPlan.Let(stepName, current, next2)
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

    def relationName(node: Node): CompilerM[SemanticError \/ String] = {
      for {
        prov <- provenanceOf(node)

        namedRel = prov.namedRelations
        relations =
          if (namedRel.size <= 1) namedRel
            else namedRel.filter(x => Path(x._1).filename == node.sql)
      } yield relations.headOption match {
                  case None => -\/(NoTableDefined(node))
                  case Some((name, _)) if (relations.size == 1) => \/-(name)
                  case _ => -\/(AmbiguousReference(node, relations.values.toList.join))
                }
    }

    def compileJoin(clause: Expr):
        CompilerM[(Mapping, Term[LogicalPlan], Term[LogicalPlan])] = {
      compile0(clause).flatMap(_ match {
        case LogicalPlan.Invoke(f: Mapping, List(left, right)) =>
          emit((f, left, right))
        case _ => fail(UnsupportedJoinCondition(clause))
      })
    }

    def compileFunction(func: Func, args: List[Expr]): CompilerM[Term[LogicalPlan]] = for {
      args <- args.map(compile0).sequenceU
    } yield func.apply(args: _*)

    def buildRecord(names: List[Option[String]], values: List[Term[LogicalPlan]]): Term[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) => LogicalPlan.Invoke(MakeObject, LogicalPlan.Constant(Data.Str(name)) :: value :: Nil)//: Term[LogicalPlan]
        case (None, value) => value
      }

      fields.reduce((a, b) => LogicalPlan.Invoke(ObjectConcat, a :: b :: Nil))
    }

    def compileArray[A <: Node](list: List[A]): CompilerM[Term[LogicalPlan]] =
      for {
        list <- list.map(compile0 _).sequenceU
      } yield MakeArrayN(list: _*)
    
    typeOf(node).flatMap(_ match {
      case Type.Const(data) => emit(LogicalPlan.Constant(data))
      case _ => 
        node match {
          case s @ SelectStmt(isDistinct, projections, relations, filter, groupBy, orderBy, limit, offset) =>
            /* 
             * 1. Joins, crosses, subselects (FROM)
             * 2. Filter (WHERE)
             * 3. Group by (GROUP BY)
             * 4. Filter (HAVING)
             * 5. Select (SELECT)
             * 6. Squash
             * 7. Sort (ORDER BY)
             * 8. Distinct (DISTINCT/DISTINCT BY)
             * 9. Drop (OFFSET)
             * 10. Take (LIMIT)
             * 11. Prune synthetic fields
             */

            // Selection of wildcards aren't named, we merge them into any other
            // objects created from other columns:
            val names = relationName(node).map(x =>
              s.namedProjections(x.toOption.map(Path(_).filename)).map {
                case (_,    Splice(_)) => None
                case (name, _)         => Some(name)
              })
            val projs = projections.map(_.expr)

            val nonSyntheticNames: CompilerM[List[String]] = names.flatMap(names => (names zip projections).map {
              case (Some(name), proj) => syntheticOf(proj).map(_ match {
                case Some(_) => None: Option[String]
                case None => Some(name)
              })
              case (None, _) => emit(None: Option[String])
            }.sequenceU.map(_.flatten))
            val anySynthetic = projections.map(syntheticOf).sequenceU.map(_.flatten.nonEmpty)
        
            relations match {
              case None => for {
                names <- names
                projs <- projs.map(compile0).sequenceU
              } yield buildRecord(names, projs)
              case Some(relations) => {
                val stepBuilder = step(relations)
                stepBuilder(Some(compile0(relations))) {
                  val filtered = filter map { filter =>
                    (CompilerState.rootTableReq |@| compile0(filter))(Filter(_, _))
                  }

                  stepBuilder(filtered) {
                    val grouped = groupBy map { groupBy =>
                      (CompilerState.rootTableReq |@| compileArray(groupBy.keys))(
                        GroupBy(_, _))
                    }

                    stepBuilder(grouped) {
                      val having = groupBy.flatMap(_.having) map { having =>
                        (CompilerState.rootTableReq |@| compile0(having))(
                          Filter(_, _))
                      }

                      stepBuilder(having) {
                        val select = Some {
                          for {
                            names <- names
                            projs <- projs.map(compile0).sequenceU
                          } yield buildRecord(names, projs)
                        }

                        stepBuilder(select) {
                          val squashed = Some(for {
                            t <- CompilerState.rootTableReq
                          } yield Squash(t))
                      
                          stepBuilder(squashed) {
                            val sort = orderBy map { orderBy =>
                              for {
                                t <- CompilerState.rootTableReq
                                keys <- orderBy.keys.map { case (key, _) => compile0(key) }.sequenceU
                                orders = orderBy.keys.map { case (_, order) => LogicalPlan.Constant(Data.Str(order.toString)) }
                              } yield OrderBy(t, MakeArrayN(keys: _*), MakeArrayN(orders: _*))
                            }

                            stepBuilder(sort) {
                              val distincted = isDistinct match {
                                  case SelectDistinct => Some {
                                    for {
                                      s <- anySynthetic
                                      t <- CompilerState.rootTableReq
                                      ns <- nonSyntheticNames
                                      projs = ns.map(name => ObjectProject(t, LogicalPlan.Constant(Data.Str(name))))
                                    } yield if (s) DistinctBy(t, MakeArrayN(projs: _*)) else Distinct(t)
                                  }
                                  case _ => None
                                }

                              stepBuilder(distincted) {
                                val drop = offset map { offset =>
                                  for {
                                    t <- CompilerState.rootTableReq
                                  } yield Drop(t, LogicalPlan.Constant(Data.Int(offset)))
                                }

                                stepBuilder(drop) {
                                  val limited = limit map { limit =>
                                    for {
                                      t <- CompilerState.rootTableReq
                                    } yield Take(t, LogicalPlan.Constant(Data.Int(limit)))
                                  }

                                  stepBuilder(limited) {
                                    val pruned = for {
                                      s <- anySynthetic
                                    } yield
                                      if (s) Some {
                                        for {
                                          t <- CompilerState.rootTableReq
                                          ns <- nonSyntheticNames
                                          ts = ns.map(name => ObjectProject(t, LogicalPlan.Constant(Data.Str(name))))
                                        } yield if (ns.isEmpty) t else buildRecord(ns.map(name => Some(name)), ts)
                                      }
                                      else None

                                    pruned.flatMap(stepBuilder(_) {
                                      CompilerState.rootTableReq
                                    })
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

            values.map((Data.Set.apply _) andThen (LogicalPlan.Constant.apply))

          case Splice(expr) =>
            expr.fold(for {
              tableOpt <- CompilerState.fullTable
              table    <- tableOpt.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))
            } yield table)(
              compile0(_))

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

          case Ident(name) => 
            for {
              relName <- relationName(node)
              rName <- relName.fold(fail(_), emit(_))
              table <- CompilerState.subtableReq(rName)
              plan  <- if (Path(rName).filename == name) emit(table) // Identifier is name of table, so just emit table plan
                       else emit(LogicalPlan.Invoke(ObjectProject, table :: LogicalPlan.Constant(Data.Str(name)) :: Nil)) // Identifier is field
            } yield plan

          case InvokeFunction(Like.name, List(expr, pattern, escape)) =>
            pattern match {
              case StringLiteral(str) =>
                escape match {
                  case StringLiteral(esc) =>
                    if (esc.length > 1)
                      fail(GenericError("escape character is not a single character"))
                    else
                      compile0(expr).map(x =>
                        LogicalPlan.Invoke(Search,
                          List(x, LogicalPlan.Constant(Data.Str(regexForLikePattern(str, esc.headOption))))))
                  case x => fail(ExpectedLiteral(x))
                }
              case x => fail(ExpectedLiteral(x))
            }

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
                            } yield (LogicalPlan.Invoke(relations.Eq, expr :: cse :: Nil), expr2) 
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

          case IntLiteral(value) => emit(LogicalPlan.Constant(Data.Int(value)))

          case FloatLiteral(value) => emit(LogicalPlan.Constant(Data.Dec(value)))

          case StringLiteral(value) => emit(LogicalPlan.Constant(Data.Str(value)))

          case BoolLiteral(value) => emit(LogicalPlan.Constant(Data.Bool(value)))

          case NullLiteral() => emit(LogicalPlan.Constant(Data.Null))

          case TableRelationAST(name, _) => emit(LogicalPlan.Read(Path(name)))

          case SubqueryRelationAST(subquery, _) => compile0(subquery)

          case JoinRelation(left, right, tpe, clause) => 
            for {
              leftName <- CompilerState.freshName("left")
              rightName <- CompilerState.freshName("right")
              leftFree = LogicalPlan.Free(leftName)
              rightFree = LogicalPlan.Free(rightName)
              left0 <- compile0(left)
              right0 <- compile0(right)
              join <- CompilerState.contextual(
                tableContext(leftFree, left) ++ tableContext(rightFree, right)
              ) {
                for {
                  tuple  <- compileJoin(clause)
                } yield LogicalPlan.Join(leftFree, rightFree,
                  tpe match {
                    case LeftJoin  => LogicalPlan.JoinType.LeftOuter
                    case InnerJoin => LogicalPlan.JoinType.Inner
                    case RightJoin => LogicalPlan.JoinType.RightOuter
                    case FullJoin  => LogicalPlan.JoinType.FullOuter
                  }, tuple._1, tuple._2, tuple._3)
              }
            } yield LogicalPlan.Let(leftName, left0,
              LogicalPlan.Let(rightName, right0, join))

          case CrossRelation(left, right) =>
            for {
              left  <- compile0(left)
              right <- compile0(right)
            } yield Cross(left, right)

          case _ => fail(NonCompilableNode(node))
        }
    })
  }

  def compile(tree: AnnotatedTree[Node, Annotations])(implicit F: Monad[F]): F[SemanticError \/ Term[LogicalPlan]] = {
    compile0(tree.root).eval(CompilerState(tree)).run
  }
}

object Compiler {
  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def id = apply[Id.Id]

  def trampoline = apply[Free.Trampoline]

  def compile(tree: AnnotatedTree[Node, Annotations]): SemanticError \/ Term[LogicalPlan] = {
    trampoline.compile(tree).run
  }
}
