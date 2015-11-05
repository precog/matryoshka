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
import quasar.recursionschemes._, Recursive.ops._, FunctorT.ops._
import quasar.fp._
import quasar.analysis._, SemanticAnalysis._, SemanticError._
import quasar.fs.Path
import quasar.sql._
import quasar.std.StdLib._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz.{Tree => _, _}, Scalaz._
import shapeless.contrib.scalaz.instances.deriveEqual

trait Compiler[F[_]] {
  import identity._
  import set._
  import string._
  import structural._

  import SemanticAnalysis.Annotations

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerM[A] = StateT[M, CompilerState, A]

  private def syntheticOf(node: Expr)(implicit m: Monad[F]):
      CompilerM[List[Option[Synthetic]]] =
    attr(node).map(_._1)

  private def provenanceOf(node: Expr)(implicit m: Monad[F]):
      CompilerM[Provenance] =
    attr(node).map(_._2)

  private final case class TableContext(
    root: Option[Fix[LogicalPlan]],
    full: () => Fix[LogicalPlan],
    subtables: Map[String, Fix[LogicalPlan]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => ObjectConcat(this.full(), that.full()),
        this.subtables ++ that.subtables)
  }

  private final case class CompilerState(
    tree:         AnnotatedTree[Expr, Annotations],
    fields:       List[String],
    tableContext: List[TableContext],
    nameGen:      Int)

  private object CompilerState {
    /** Runs a computation inside a table context, which contains compilation
      * data for the tables in scope.
      */
    def contextual[A](t: TableContext)(f: CompilerM[A])(implicit m: Monad[F]):
        CompilerM[A] =
      mod((s: CompilerState) => s.copy(tableContext = t :: s.tableContext)) *>
        f <*
        mod((s: CompilerState) => s.copy(tableContext = s.tableContext.drop(1)))

    def addFields[A](add: List[String])(f: CompilerM[A])(implicit m: Monad[F]):
        CompilerM[A] =
      for {
        curr <- read[CompilerState, List[String]](_.fields)
        _    <- mod((s: CompilerState) => s.copy(fields = curr ++ add))
        a <- f
      } yield a

    def fields(implicit m: Monad[F]): CompilerM[List[String]] =
      read[CompilerState, List[String]](_.fields)

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]](_.tableContext.headOption.flatMap(_.root))

    def rootTableReq(implicit m: Monad[F]): CompilerM[Fix[LogicalPlan]] =
      rootTable.flatMap(_.map(emit).getOrElse(fail(CompiledTableMissing)))

    def subtable(name: String)(implicit m: Monad[F]):
        CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]](_.tableContext.headOption.flatMap(_.subtables.get(name)))

    def subtableReq(name: String)(implicit m: Monad[F]):
        CompilerM[Fix[LogicalPlan]] =
      subtable(name).flatMap(
        _.map(emit).getOrElse(fail(CompiledSubtableMissing(name))))

    def fullTable(implicit m: Monad[F]): CompilerM[Option[Fix[LogicalPlan]]] =
      read[CompilerState, Option[Fix[LogicalPlan]]](_.tableContext.headOption.map(_.full()))

    /** Generates a fresh name for use as an identifier, e.g. tmp321. */
    def freshName(prefix: String)(implicit m: Monad[F]): CompilerM[Symbol] =
      read[CompilerState, Int](_.nameGen).map(n => Symbol(prefix + n.toString)) <*
        mod((s: CompilerState) => s.copy(nameGen = s.nameGen + 1))
  }

  sealed trait JoinDir
  final case object Left extends JoinDir {
    override def toString: String = "left"
  }
  final case object Right extends JoinDir {
    override def toString: String = "right"
  }

  private def read[A, B](f: A => B)(implicit m: Monad[F]):
      StateT[M, A, B] =
    StateT((s: A) => (s, f(s)).point[M])

  private def attr(node: Expr)(implicit m: Monad[F]): CompilerM[Annotations] =
    read(_.tree.attr(node))

  private def tree(implicit m: Monad[F]):
      CompilerM[AnnotatedTree[Expr, Annotations]] =
    read(_.tree)

  private def fail[A](error: SemanticError)(implicit m: Applicative[F]):
      CompilerM[A] =
    lift(error.left)

  private def emit[A](value: A)(implicit m: Applicative[F]): CompilerM[A] =
    lift(value.right)

  private def lift[A](v: SemanticError \/ A)(implicit m: Applicative[F]):
      CompilerM[A] =
    StateT[M, CompilerState, A]((s: CompilerState) =>
      EitherT.eitherT(v.map(s -> _).point[F]))

  private def whatif[S, A](f: StateT[M, S, A])(implicit m: Monad[F]):
      StateT[M, S, A] =
    read((s: S) => s).flatMap(oldState => f.imap(κ(oldState)))

  private def mod(f: CompilerState => CompilerState)(implicit m: Monad[F]):
      CompilerM[Unit] =
    StateT[M, CompilerState, Unit](s => (f(s), ()).point[M])

  // TODO: parameterize this
  val library = std.StdLib

  // CORE COMPILER
  private def compile0(node: Expr)(implicit M: Monad[F]):
      CompilerM[Fix[LogicalPlan]] = {
    def findFunction(name: String) =
      library.functions.find(f => f.name.toLowerCase == name.toLowerCase).fold[CompilerM[Func]](
        fail(FunctionNotFound(name)))(
        emit(_))

    def compileCases(cases: List[Case[Expr]], default: Fix[LogicalPlan])(f: Case[Expr] => CompilerM[(Fix[LogicalPlan], Fix[LogicalPlan])]) =
      cases.traverseU(f).map(_.foldRight(default) {
        case ((cond, expr), default) => relations.Cond(cond, expr, default)
      })

    def regexForLikePattern(pattern: String, escapeChar: Option[Char]):
        String = {
      def sansEscape(pat: List[Char]): List[Char] = pat match {
        case '_' :: t =>         '.' +: escape(t)
        case '%' :: t => ".*".toList ⊹ escape(t)
        case c   :: t =>
          if ("\\^$.|?*+()[{".contains(c))
            '\\' +: c +: escape(t)
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

    def flattenJoins(term: Fix[LogicalPlan], relations: SqlRelation[Expr]):
        Fix[LogicalPlan] = relations match {
      case _: NamedRelation[_] => term
      case JoinRelation(left, right, _, _) =>
        ObjectConcat(
          flattenJoins(ObjectProject(term, LogicalPlan.Constant(Data.Str("left"))), left),
          flattenJoins(ObjectProject(term, LogicalPlan.Constant(Data.Str("right"))), right))
    }

    def buildJoinDirectionMap(relations: SqlRelation[Expr]):
        Map[String, List[JoinDir]] = {
      def loop(rel: SqlRelation[Expr], acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation[_] => Map(t.aliasName -> acc)
        case JoinRelation(left, right, tpe, clause) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }

      loop(relations, Nil)
    }

    def compileTableRefs(joined: Fix[LogicalPlan], relations: SqlRelation[Expr]):
        Map[String, Fix[LogicalPlan]] =
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(
            joined)(
            (dir, acc) =>
              ObjectProject(
                acc,
                LogicalPlan.Constant(Data.Str(dir.toString))))
      }

    def tableContext(joined: Fix[LogicalPlan], relations: SqlRelation[Expr]):
        TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[Expr]):
        (Option[CompilerM[Fix[LogicalPlan]]] =>
          CompilerM[Fix[LogicalPlan]] =>
          CompilerM[Fix[LogicalPlan]]) = {
      (current: Option[CompilerM[Fix[LogicalPlan]]]) =>
      (next: CompilerM[Fix[LogicalPlan]]) =>
      current.map { current =>
        for {
          stepName <- CompilerState.freshName("tmp")
          current  <- current
          next2    <- CompilerState.contextual(tableContext(LogicalPlan.Free(stepName), relations))(next)
        } yield LogicalPlan.Let(stepName, current, next2)
      }.getOrElse(next)
    }

    def relationName(node: Expr): CompilerM[SemanticError \/ String] =
      provenanceOf(node).map(prov => {
        val namedRel = prov.namedRelations
        val relations =
          if (namedRel.size <= 1) namedRel
          else {
            val filtered = namedRel.filter(x => Path(x._1).filename ≟ pprint(node))
            if (filtered.isEmpty) namedRel else filtered
          }
        relations.toList match {
          case Nil             => -\/ (NoTableDefined(node))
          case List((name, _)) =>  \/-(name)
          case x               => -\/ (AmbiguousReference(node, x.map(_._2).join))
        }
      })

    def compileFunction(func: Func, args: List[Expr]):
        CompilerM[Fix[LogicalPlan]] =
      args.traverseU(compile0).map(func.apply(_: _*))

    def buildRecord(names: List[Option[String]], values: List[Fix[LogicalPlan]]):
        Fix[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) =>
          MakeObject(LogicalPlan.Constant(Data.Str(name)), value)
        case (None, value) => value
      }

      // TODO: If we had an optimization pass that included eliding an
      //       ObjectConcat with an empty map on one side, this could be done
      //       in a single foldLeft.
      fields match {
        case Nil => LogicalPlan.Constant(Data.Obj(Map()))
        case x :: xs =>
          xs.foldLeft(x)((a, b) =>
            LogicalPlan.Invoke(ObjectConcat, a :: b :: Nil))
      }
    }

    def compileRelation(r: SqlRelation[Expr]): CompilerM[Fix[LogicalPlan]] =
      r match {
        case TableRelationAST(name, _) => emit(LogicalPlan.Read(Path(name)))
        case ExprRelationAST(expr, _) => compile0(expr)
        case JoinRelation(left, right, tpe, clause) =>
          for {
            leftName <- CompilerState.freshName("left")
            rightName <- CompilerState.freshName("right")
            leftFree = LogicalPlan.Free(leftName)
            rightFree = LogicalPlan.Free(rightName)
            left0 <- compileRelation(left)
            right0 <- compileRelation(right)
            join <- CompilerState.contextual(
              tableContext(leftFree, left) ++ tableContext(rightFree, right))(
              compile0(clause).map(c =>
                LogicalPlan.Invoke(
                  tpe match {
                    case LeftJoin  => LeftOuterJoin
                    case sql.InnerJoin => InnerJoin
                    case RightJoin => RightOuterJoin
                    case FullJoin  => FullOuterJoin
                  },
                  List(leftFree, rightFree, c))))
          } yield LogicalPlan.Let(leftName, left0,
            LogicalPlan.Let(rightName, right0, join))
      }

    node match {
      case s @ Select(isDistinct, projections, relations, filter, groupBy, orderBy) =>
        /* 1. Joins, crosses, subselects (FROM)
         * 2. Filter (WHERE)
         * 3. Group by (GROUP BY)
         * 4. Filter (HAVING)
         * 5. Select (SELECT)
         * 6. Squash
         * 7. Sort (ORDER BY)
         * 8. Distinct (DISTINCT/DISTINCT BY)
         * 9. Prune synthetic fields
         */

        // Selection of wildcards aren't named, we merge them into any other
        // objects created from other columns:
        val names: CompilerM[List[Option[String]]] =
          relationName(node).map(x =>
            namedProjections(s, x.toOption.map(Path(_).filename)).map {
              case (_,    Splice(_)) => None
              case (name, _)         => Some(name)
            })
        val projs = projections.map(_.expr)

        val syntheticNames: CompilerM[List[String]] =
          (names ⊛ syntheticOf(s))(
            _.zip(_).flatMap {
              case (Some(name), Some(_)) => List(name)
              case (_,          _)       => Nil
            })

        relations.fold(
          (names ⊛ projs.traverseU(compile0))(buildRecord))(
          relations => {
            val stepBuilder = step(relations)
            stepBuilder(compileRelation(relations).some) {
              val filtered = filter.map(filter =>
                (CompilerState.rootTableReq ⊛ compile0(filter))(Filter(_, _)))

              stepBuilder(filtered) {
                val grouped = groupBy.map(groupBy =>
                  (CompilerState.rootTableReq ⊛
                    groupBy.keys.traverseU(compile0))((src, keys) =>
                    GroupBy(src, MakeArrayN(keys: _*))))

                stepBuilder(grouped) {
                  val having = groupBy.flatMap(_.having).map(having =>
                    (CompilerState.rootTableReq ⊛ compile0(having))(
                      Filter(_, _)))

                  stepBuilder(having) {
                    val select =
                      (names ⊛ CompilerState.rootTableReq ⊛ projs.traverseU(compile0))((names, t, projs) =>
                        buildRecord(
                          names,
                          projs.map(p => p.unFix match {
                            case LogicalPlan.ConstantF(_) => Constantly(p, t)
                            case _                        => p
                          })))

                    val squashed = select.map(Squash(_))

                    stepBuilder(squashed.some) {
                      val sort = orderBy.map(orderBy =>
                        for {
                          t <- CompilerState.rootTableReq
                          names <- names
                          flat = names.foldMap(_.toList)
                          keys <- CompilerState.addFields(flat)(orderBy.keys.traverseU { case (_, key) => compile0(key) })
                          orders = orderBy.keys.map { case (order, _) => LogicalPlan.Constant(Data.Str(order.toString)) }
                        } yield OrderBy(t, MakeArrayN(keys: _*), MakeArrayN(orders: _*)))

                      stepBuilder(sort) {
                        val distincted = isDistinct match {
                          case SelectDistinct =>
                            (syntheticNames ⊛ CompilerState.rootTableReq)(
                              (ns, t) =>
                              if (ns.nonEmpty)
                                DistinctBy(t, ns.foldLeft(t)((acc, field) =>
                                  DeleteField(acc, LogicalPlan.Constant(Data.Str(field)))))
                              else Distinct(t)).some
                          case _ => None
                        }

                        stepBuilder(distincted) {
                          val pruned =
                            syntheticNames.flatMap(ns =>
                              CompilerState.rootTableReq.map(
                                ns.foldLeft(_)((acc, field) =>
                                  DeleteField(acc,
                                    LogicalPlan.Constant(Data.Str(field))))))

                          pruned
                        }
                      }
                    }
                  }
                }
              }
            }
          })

      case SetLiteral(values0) =>
        val values = values0.traverseU {
          case IntLiteral(v) => emit[Data](Data.Int(v))
          case FloatLiteral(v) => emit[Data](Data.Dec(v))
          case StringLiteral(v) => emit[Data](Data.Str(v))
          case x => fail[Data](ExpectedLiteral(x))
        }

        values.map(arr => LogicalPlan.Constant(Data.Set(arr)))

      case ArrayLiteral(exprs) =>
        exprs.traverseU(compile0).map(MakeArrayN(_: _*))

      case Splice(expr) =>
        expr.fold(
          CompilerState.fullTable.flatMap(_.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))))(
          compile0)

      case Binop(left, right, op) =>
        findFunction(op.name).flatMap(compileFunction(_, left :: right :: Nil))

      case Unop(expr, op) =>
        findFunction(op.name).flatMap(compileFunction(_, expr :: Nil))

      case Ident(name) =>
        CompilerState.fields.flatMap(fields =>
          if (fields.any(_ == name))
            CompilerState.rootTableReq.map(
              ObjectProject(_, LogicalPlan.Constant(Data.Str(name))))
          else
            for {
              relName <- relationName(node)
              rName   <- relName.fold(fail, emit)
              table   <- CompilerState.subtableReq(rName)
            } yield
              if (Path(rName).filename ≟ name) table
              else ObjectProject(table, LogicalPlan.Constant(Data.Str(name))))

      case InvokeFunction(Like.name, List(expr, pattern, escape)) =>
        pattern match {
          case StringLiteral(str) =>
            escape match {
              case StringLiteral(esc) =>
                if (esc.length > 1)
                  fail(GenericError("escape character is not a single character"))
                else
                  compile0(expr).map(
                    Search(_,
                      LogicalPlan.Constant(Data.Str(regexForLikePattern(str, esc.headOption))),
                      LogicalPlan.Constant(Data.Bool(false))))
              case x => fail(ExpectedLiteral(x))
            }
          case x => fail(ExpectedLiteral(x))
        }

      case InvokeFunction(name, args) =>
        findFunction(name).flatMap(compileFunction(_, args))

      case Match(expr, cases, default0) =>
        for {
          expr    <- compile0(expr)
          default <- default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0)
          cases   <- compileCases(cases, default) {
            case Case(cse, expr2) =>
              (compile0(cse) ⊛ compile0(expr2))((cse, expr2) =>
                (relations.Eq(expr, cse), expr2))
          }
        } yield cases

      case Switch(cases, default0) =>
        default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0).flatMap(
          compileCases(cases, _) {
            case Case(cond, expr2) =>
              (compile0(cond) ⊛ compile0(expr2))((_, _))
          })

      case IntLiteral(value) => emit(LogicalPlan.Constant(Data.Int(value)))
      case FloatLiteral(value) => emit(LogicalPlan.Constant(Data.Dec(value)))
      case StringLiteral(value) => emit(LogicalPlan.Constant(Data.Str(value)))
      case BoolLiteral(value) => emit(LogicalPlan.Constant(Data.Bool(value)))
      case NullLiteral() => emit(LogicalPlan.Constant(Data.Null))
      case Vari(name) => emit(LogicalPlan.Free(Symbol(name)))
    }
  }

  def compile(tree: AnnotatedTree[Expr, Annotations])(implicit F: Monad[F]): F[SemanticError \/ Fix[LogicalPlan]] = {
    compile0(tree.root).eval(CompilerState(tree, Nil, Nil, 0)).run.map(_.map(Compiler.reduceGroupKeys))
  }
}

object Compiler {
  import LogicalPlan._

  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def trampoline = apply[scalaz.Free.Trampoline]

  def compile(tree: AnnotatedTree[Expr, Annotations]):
      SemanticError \/ Fix[LogicalPlan] =
    trampoline.compile(tree).run

  def reduceGroupKeys(tree: Fix[LogicalPlan]): Fix[LogicalPlan] = {
    // Step 0: identify key expressions, and rewrite them by replacing the
    // group source with the source at the point where they might appear.
    def keysƒ(t: LogicalPlan[(Fix[LogicalPlan], List[Fix[LogicalPlan]])]):
        (Fix[LogicalPlan], List[Fix[LogicalPlan]]) =
    {
      def groupedKeys(t: LogicalPlan[Fix[LogicalPlan]], newSrc: Fix[LogicalPlan]): Option[List[Fix[LogicalPlan]]] = {
        t match {
          case InvokeF(set.GroupBy, List(src, structural.MakeArrayN(keys))) =>
            Some(keys.map(_.transform(t => if (t ≟ src) newSrc else t)))
          case InvokeF(Sifting(_, _, _, _, _, _, _), src :: _) =>
            groupedKeys(src.unFix, newSrc)
          case _ => None
        }
      }

      (Fix(t.map(_._1)),
        groupedKeys(t.map(_._1), Fix(t.map(_._1))).getOrElse(t.foldMap(_._2)))
    }
    val keys: List[Fix[LogicalPlan]] = boundCata(tree)(keysƒ)._2

    // Step 1: annotate nodes containing the keys.
    val ann: Cofree[LogicalPlan, Boolean] = boundAttribute(tree)(keys contains _)

    // Step 2: transform from the top, inserting Arbitrary where a key is not
    // otherwise reduced.
    def rewriteƒ(t: Cofree[LogicalPlan, Boolean]): LogicalPlan[Cofree[LogicalPlan, Boolean]] = {
      def strip(v: Cofree[LogicalPlan, Boolean]) = Cofree(false, v.tail)

      t.tail match {
        case InvokeF(func @ Reduction(_, _, _, _, _, _, _), arg :: Nil) =>
          InvokeF(func, List(strip(arg)))

        case _ =>
          if (t.head) InvokeF(agg.Arbitrary, List(strip(t)))
          else t.tail
      }
    }
    Corecursive[Fix].ana(ann)(rewriteƒ)
  }
}
