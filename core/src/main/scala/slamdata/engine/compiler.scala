package slamdata.engine

import scalaz._
import Scalaz._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import slamdata.engine.analysis._
import SemanticAnalysis._
import SemanticError._

import slamdata.engine.fp._
import slamdata.engine.sql._
import slamdata.engine.fs.Path
import slamdata.engine.analysis.fixplate._

import slamdata.engine.std.StdLib._

trait Compiler[F[_]] {
  import agg._
  import identity._
  import math._
  import relations._
  import set._
  import string._
  import structural._

  import SemanticAnalysis.Annotations

  type FreeAnn = Free[LogicalPlan, AnnSql]

  // I don’t know why this is private in Free.scala.
  def suspendF[S[_]: Functor, A](fa: S[Free[S, A]]) = Free.liftF(fa).join
  // aaaaaand, specialized :(
  val pointLP = Free.point[LogicalPlan, AnnSql] _
  val liftLP = Free.liftF[LogicalPlan, AnnSql] _
  val suspendLP = suspendF[LogicalPlan, AnnSql] _

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerM[A] = StateT[M, CompilerState, A]

  private def typeOf(node: AnnSql): Type = node.head._2

  private def provenanceOf(node: AnnSql): Provenance =
    node.head._1._1._2

  private def syntheticOf(node: AnnSql): Option[Synthetic] =
    node.head._1._1._1

  private def funcOf(node: AnnSql): SemanticError \/ Func =
    node.head._1._2.fold[SemanticError \/ Func](
      -\/(FunctionNotBound(node)))(
      \/-(_))

  private case class TableContext(
    root: Option[FreeAnn],
    full: () => FreeAnn,
    subtables: Map[String, FreeAnn]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => suspendLP(ObjectConcat(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private case class CompilerState(
    tableContext: List[TableContext] = Nil,
    nameGen:      Int = 0
  )

  private object CompilerState {
    /**
     * Runs a computation inside a table context, which contains compilation
     * data for the tables in scope.
     */
    def contextual[A](t: TableContext)(f: CompilerM[A])(implicit F: Monad[F]):
        CompilerM[A] = for {
      _ <- mod((s: CompilerState) => s.copy(tableContext = t :: s.tableContext))
      a <- f
      _ <- mod((s: CompilerState) => s.copy(tableContext = s.tableContext.drop(1)))
    } yield a

    def rootTable(implicit F: Monad[F]): CompilerM[Option[FreeAnn]] =
      read[CompilerState, Option[FreeAnn]](_.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit F: Monad[F]): CompilerM[FreeAnn] = {
      this.rootTable flatMap {
        case Some(t)  => emit(t)
        case None     => fail(CompiledTableMissing)
      }
    }

    def subtable(name: String)(implicit F: Monad[F]):
        CompilerM[Option[FreeAnn]] =
      read[CompilerState, Option[FreeAnn]](_.tableContext.headOption.flatMap(_.subtables.get(name)))

    def subtableReq(name: String)(implicit F: Monad[F]): CompilerM[FreeAnn] =
      subtable(name) flatMap {
        case Some(t) => emit(t)
        case None    => fail(CompiledSubtableMissing(name))
      }

    def fullTable(implicit F: Monad[F]): CompilerM[Option[FreeAnn]] =
      read[CompilerState, Option[FreeAnn]](_.tableContext.headOption.map(_.full()))

    def fullTableReq(implicit F: Monad[F]): CompilerM[FreeAnn] =
      fullTable flatMap {
        case Some(t) => emit(t)
        case None    => fail(CompiledTableMissing)
      }

    /**
     * Generates a fresh name for use as an identifier, e.g. tmp321.
     */
    def freshName(prefix: String)(implicit F: Monad[F]): CompilerM[Symbol] =
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

  private def read[A, B](f: A => B)(implicit F: Monad[F]): StateT[M, A, B] =
    StateT((s: A) => Applicative[M].point((s, f(s))))

  private def fail[A](error: SemanticError)(implicit F: Applicative[F]):
      CompilerM[A] =
    lift(-\/(error))

  private def emit[A](value: A)(implicit F: Applicative[F]): CompilerM[A] =
    lift(\/-(value))

  private def lift[A](v: SemanticError \/ A)(implicit F: Applicative[F]):
      CompilerM[A] =
    StateT[M, CompilerState, A]((s: CompilerState) =>
      EitherT.eitherT(F.point(v.map(s -> _))))

  private def whatif[S, A](f: StateT[M, S, A])(implicit F: Monad[F]):
      StateT[M, S, A] =
    for {
      oldState <- read((s: S) => s)
      rez      <- f.imap(κ(oldState))
    } yield rez

  private def mod(f: CompilerState => CompilerState)(implicit F: Monad[F]):
      CompilerM[Unit] =
    StateT[M, CompilerState, Unit](s => Applicative[M].point(f(s) -> Unit))

  private def invoke(func: Func, args: List[AnnSql]): FreeAnn =
    liftLP(func(args: _*))

  // CORE COMPILER
  private def compileƒ(node: AnnSql)(implicit F: Monad[F]): CompilerM[FreeAnn] = {
    def compileCases(cases: List[Case[AnnSql]], default: FreeAnn)(f: Case[AnnSql] => (AnnSql, AnnSql)) =
      cases.foldRight[FreeAnn](default)((cas, default) =>
        ((x : (AnnSql, AnnSql)) =>
          suspendLP(relations.Cond(pointLP(x._1), pointLP(x._2), default)))(f(cas)))

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

    def flattenJoins(term: FreeAnn, relations: SqlRelation[AnnSql]): FreeAnn = {
      def flattenSide(name: String, side: SqlRelation[AnnSql]) =
        flattenJoins(suspendLP(ObjectProject(term, liftLP(LogicalPlan.ConstantF(Data.Str(name))))), side)

      def flattenJoin(l: SqlRelation[AnnSql], r: SqlRelation[AnnSql]) =
        suspendLP(ObjectConcat(flattenSide("left", l), flattenSide("right", r)))

      relations match {
        case JoinRelation(left, right, _, _) => flattenJoin(left, right)
        case CrossRelation(left, right)      => flattenJoin(left, right)
        case _                               => term
      }
    }

    def buildJoinDirectionMap(relations: SqlRelation[_]): Map[String, List[JoinDir]] = {
      def loop(rel: SqlRelation[_], acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation[_] => Map(t.aliasName -> acc)
        case JoinRelation(left, right, _, _) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
        case CrossRelation(left, right) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }

      loop(relations, Nil)
    }

    def compileTableRefs(joined: FreeAnn, relations: SqlRelation[AnnSql]): Map[String, FreeAnn] = {
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(joined)((dir, acc) =>
            suspendLP(ObjectProject(acc, liftLP(LogicalPlan.ConstantF(Data.Str(dir.toString))))))
      }
    }

    def tableContext(joined: FreeAnn, relations: SqlRelation[AnnSql]):
        TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[AnnSql]):
        (Option[CompilerM[FreeAnn]] =>
          CompilerM[FreeAnn] =>
          CompilerM[FreeAnn]) = {
      (current: Option[CompilerM[FreeAnn]]) =>
      (next: CompilerM[FreeAnn]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          next2    <- CompilerState.contextual(tableContext(liftLP(LogicalPlan.FreeF(stepName)), relations))(next)
        } yield suspendLP(LogicalPlan.LetF(stepName, current, next2))
      }.getOrElse(next)
    }

    def relationName(node: AnnSql): SemanticError \/ String = {
      val namedRel = provenanceOf(node).namedRelations
      val relations =
        if (namedRel.size <= 1) namedRel
        else namedRel.filter(x => Path(x._1).filename == node.sql)
      relations.toList match {
        case Nil => -\/(NoTableDefined(node))
        case List(Some((name, _))) => \/-(name)
        case _ => -\/(AmbiguousReference(node, relations.values.toList.join))
      }
    }

    def compileRelation(rel: SqlRelation[AnnSql]):
        CompilerM[FreeAnn] =
      rel match {
        case TableRelationAST(name, _) =>
          emit(liftLP(LogicalPlan.ReadF(Path(name))))
        case ExprRelationAST(expr, _) => emit(pointLP(expr))
        case CrossRelation(left, right) =>
          (compileRelation(left) |@| compileRelation(right))((l, r) =>
            suspendLP(Cross(l, r)))
        case JoinRelation(left, right, tpe, clause) =>
          for {
            leftName <- CompilerState.freshName("left")
            rightName <- CompilerState.freshName("right")
            leftFree = liftLP(LogicalPlan.FreeF(leftName))
            rightFree = liftLP(LogicalPlan.FreeF(rightName))
            left0 <- compileRelation(left)
            right0 <- compileRelation(right)
            join <- CompilerState.contextual(
              tableContext(leftFree, left) ++ tableContext(rightFree, right)
            ) {
              for {
                tuple  <- compileJoin(clause, leftFree, rightFree)
              } yield suspendLP(LogicalPlan.JoinF(leftFree, rightFree,
                tpe match {
                  case LeftJoin  => LogicalPlan.JoinType.LeftOuter
                  case InnerJoin => LogicalPlan.JoinType.Inner
                  case RightJoin => LogicalPlan.JoinType.RightOuter
                  case FullJoin  => LogicalPlan.JoinType.FullOuter
                }, tuple._1, tuple._2, tuple._3))
            }
          } yield suspendLP(LogicalPlan.LetF(leftName, left0,
            suspendLP(LogicalPlan.LetF(rightName, right0, join))))
      }

    def compileJoin(clause: AnnSql, lt: FreeAnn, rt: FreeAnn):
        CompilerM[(Mapping, FreeAnn, FreeAnn)] =
      compileƒ(clause).flatMap(_.resume.fold({
        case LogicalPlan.InvokeF(f: Mapping, List(left, right)) =>
          if (Tag.unwrap(left.foldMap(x => Tags.Disjunction(x == lt))) &&
            Tag.unwrap(right.foldMap(x => Tags.Disjunction(x == rt))))
            emit((f, left, right))
          else if (Tag.unwrap(left.foldMap(x => Tags.Disjunction(x == rt))) &&
            Tag.unwrap(right.foldMap(x => Tags.Disjunction(x == lt))))
            flip(f).fold[CompilerM[(Mapping, FreeAnn, FreeAnn)]](
              fail(UnsupportedJoinCondition(clause)))(
              x => emit((x, right, left)))
          else fail(UnsupportedJoinCondition(clause))
        case _ => fail(UnsupportedJoinCondition(clause))
      },
        κ(fail(UnsupportedJoinCondition(clause)))))

    def compileFunction(func: Func, args: List[AnnSql]): FreeAnn =
      liftLP(func(args: _*))

    def buildRecord(names: List[Option[String]], values: List[AnnSql]): FreeAnn =
      names.zip(values).foldLeft(
        liftLP(LogicalPlan.ConstantF(Data.Set(Nil))))(
        (a, b) => suspendLP(ObjectConcat(a, b match {
          case (Some(name), value) =>
            suspendLP(
              MakeObject(
                liftLP(LogicalPlan.ConstantF(Data.Str(name))),
                pointLP(value)))
          case (None, value) => pointLP(value)
        })))

    def compileArray(list: List[AnnSql]): FreeAnn = liftLP(MakeArrayN(list: _*))

    typeOf(node) match {
      case Type.Const(data) => emit(liftLP(LogicalPlan.ConstantF(data)))
      case typ =>
        node.tail match {
          case Select(isDistinct, projections, relations, filter, groupBy, orderBy, limit, offset) =>
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

            // Selection of wildcards aren't named, we merge them into any
            // other objects created from other columns:
            val names =
              Expr.namedProjections(
                forget(node),
                relationName(node).toOption.map(Path(_).filename)).map {
                case (_,    Splice(_)) => None
                case (name, _)         => Some(name)
              }
            val projs = projections.map(_.expr)

            val syntheticNames: List[String] =
              (names zip projections).map {
                case (Some(name), proj) => syntheticOf(proj).map(_ match {
                  case Some(_) => Some(name)
                  case None => None: Option[String]
                })
                case (None, _) => emit(None: Option[String])
              }.sequenceU.map(_.flatten)

            relations match {
              case None => emit(buildRecord(names, projs))
              case Some(relations) => {
                val stepBuilder = step(relations)
                stepBuilder(Some(compileRelation(relations))) {
                  val filtered = filter.map(filter =>
                    CompilerState.rootTableReq.map(x =>
                      suspendLP(Filter(x, pointLP(filter)))))

                  stepBuilder(filtered) {
                    val grouped = groupBy map { groupBy =>
                      CompilerState.rootTableReq.map(x =>
                        suspendLP(GroupBy(x, compileArray(groupBy.keys))))
                    }

                    stepBuilder(grouped) {
                      val having = groupBy.flatMap(_.having) map { having =>
                        CompilerState.rootTableReq.map(x =>
                          suspendLP(Filter(x, pointLP(having))))
                      }

                      stepBuilder(having) {
                        val select = Some(emit(buildRecord(names, projs)))

                        stepBuilder(select) {
                          val squashed = Some(for {
                            t <- CompilerState.rootTableReq
                          } yield suspendLP(Squash(t)))

                          stepBuilder(squashed) {
                            val sort = orderBy map { orderBy =>
                              for {
                                t <- CompilerState.rootTableReq
                                keys = orderBy.keys.map(_._2)
                                orders = orderBy.keys.map(k => liftLP(LogicalPlan.ConstantF(Data.Str(k._1.toString))))
                              } yield suspendLP(OrderBy(t, liftLP(MakeArrayN(keys: _*)), suspendLP(MakeArrayN(orders: _*))))
                            }

                            stepBuilder(sort) {
                              val distincted = isDistinct match {
                                case SelectDistinct => Some {
                                  val ns = syntheticNames
                                  for {
                                    t <- CompilerState.rootTableReq
                                  } yield suspendLP(if (ns.nonEmpty)
                                    DistinctBy(t, ns.foldLeft(t)((acc, field) => suspendLP(DeleteField(acc, liftLP(LogicalPlan.ConstantF(Data.Str(field)))))))
                                  else Distinct(t))
                                }
                                case _ => None
                              }

                              stepBuilder(distincted) {
                                val drop = offset map { offset =>
                                  for {
                                    t <- CompilerState.rootTableReq
                                  } yield suspendLP(Drop(t, liftLP(LogicalPlan.ConstantF(Data.Int(offset)))))
                                }

                                stepBuilder(drop) {
                                  val limited = limit map { limit =>
                                    for {
                                      t <- CompilerState.rootTableReq
                                    } yield suspendLP(Take(t, liftLP(LogicalPlan.ConstantF(Data.Int(limit)))))
                                  }

                                  stepBuilder(limited) {
                                    val ns = syntheticNames
                                    val pruned =
                                      if (ns.nonEmpty)
                                        Some(CompilerState.rootTableReq.map(
                                          ns.foldLeft(_)((acc, field) =>
                                            suspendLP(DeleteField(acc,
                                              liftLP(LogicalPlan.ConstantF(Data.Str(field))))))))
                                      else None

                                    stepBuilder(
                                      pruned)(
                                      CompilerState.rootTableReq)
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

          case SetLiteral(values0) =>
            val values = (values0.map(_.tail match {
              case IntLiteral(v)    =>  \/-(Data.Int(v))
              case FloatLiteral(v)  =>  \/-(Data.Dec(v))
              case StringLiteral(v) =>  \/-(Data.Str(v))
              case x                => -\/ (ExpectedLiteral(x))
            })).sequenceU

            lift(values.map(x => liftLP(LogicalPlan.ConstantF(Data.Set(x)))))

          case ArrayLiteral(exprs) =>
            emit(exprs.foldLeft[FreeAnn](
              liftLP(LogicalPlan.ConstantF(Data.Arr(Nil))))(
              (acc, x) => suspendLP(ArrayConcat(acc, liftLP(LogicalPlan.InvokeF(MakeArray, List(x)))))))

          case Splice(expr) =>
            expr.fold(
              CompilerState.fullTable.flatMap(_.fold[CompilerM[FreeAnn]](
                fail(GenericError("Not within a table context so could not find table expression for wildcard")))(
                emit)))(
              x => emit(pointLP(x)))

          case Binop(left, right, sql.Concat) =>
            emit(typ match {
              case Type.Str         => invoke(Concat, List(left, right))
              case t if t.arrayLike => invoke(ArrayConcat, List(left, right))
              case _                => fail(GenericError("can't concat mixed/unknown types: " + left.sql + ", " + right.sql))
            })

          case Binop(left, right, op) =>
            lift(funcOf(node).map(invoke(_, left :: right :: Nil)))

          case Unop(expr, op) =>
            lift(funcOf(node).map(invoke(_, expr :: Nil)))

          case Ident(name) =>
            for {
              rName <- lift(relationName(node))
              table <- CompilerState.subtableReq(rName)
              plan  <- if (Path(rName).filename == name) emit(table) // Identifier is name of table, so just emit table plan
              else emit(suspendLP(ObjectProject(table, liftLP(LogicalPlan.ConstantF(Data.Str(name)))))) // Identifier is field
            } yield plan

          case InvokeFunction(Like.name, List(expr, pattern, escape)) =>
            (pattern.tail, escape.tail) match {
              case (StringLiteral(str), StringLiteral(esc)) =>
                if (esc.length > 1)
                  fail(GenericError("escape character is not a single character"))
                else
                  emit(suspendLP(Search(
                    pointLP(expr),
                    liftLP(LogicalPlan.ConstantF(Data.Str(regexForLikePattern(str, esc.headOption))))))).join
              case (StringLiteral(_), x) => fail(ExpectedLiteral(x))
              case (x,                _) => fail(ExpectedLiteral(x))
            }

          case InvokeFunction(name, args) =>
            lift(funcOf(node).map(compileFunction(_, args)))

          case Match(expr, cases, default) =>
            compileCases(
              cases,
              default.fold(liftLP(LogicalPlan.ConstantF(Data.Null)))(pointLP)) {
              case Case(cse, expr2) => (relations.Eq(expr, cse), expr2)
            }

          case Switch(cases, default) =>
            compileCases(
              cases,
              default.fold(liftLP(LogicalPlan.ConstantF(Data.Null)))(pointLP)) {
              case Case(cond, expr2) => (cond, expr2)
            }

          case IntLiteral(value) =>
            emit(liftLP(LogicalPlan.ConstantF(Data.Int(value))))
          case FloatLiteral(value) =>
            emit(liftLP(LogicalPlan.ConstantF(Data.Dec(value))))
          case StringLiteral(value) =>
            emit(liftLP(LogicalPlan.ConstantF(Data.Str(value))))
          case BoolLiteral(value) =>
            emit(liftLP(LogicalPlan.ConstantF(Data.Bool(value))))
          case NullLiteral =>
            emit(liftLP(LogicalPlan.ConstantF(Data.Null)))
        }
    }
  }

  def compile(tree: AnnSql)(implicit F: Monad[F]): F[SemanticError \/ Term[LogicalPlan]] = {
    futuM(tree)(compileƒ).eval.run
  }
}

object Compiler {
  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def trampoline = apply[Free.Trampoline]

  def compile(tree: Cofree[Expr, Annotations]): SemanticError \/ Term[LogicalPlan] = {
    trampoline.compile(tree).run
  }
}
