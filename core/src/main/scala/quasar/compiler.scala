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
import quasar.recursionschemes._, cofree._, Recursive.ops._, FunctorT.ops._
import quasar.fp._
import quasar.fs.Path
import quasar.sql._
import quasar.std.StdLib._
import quasar.SemanticAnalysis._, quasar.SemanticError._

import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}
import scalaz.{Tree => _, _}, Scalaz._
import shapeless.contrib.scalaz.instances.deriveEqual

trait Compiler[F[_]] {
  import identity._
  import set._
  import string._
  import structural._

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private type CompilerM[A] = StateT[M, CompilerState, A]

  private def syntheticOf(node: CoExpr): List[Option[Synthetic]] =
    node.head._1

  private def provenanceOf(node: CoExpr): Provenance =
    node.head._2

  private final case class TableContext(
    root: Option[Fix[LogicalPlan]],
    full: () => Fix[LogicalPlan],
    subtables: Map[String, Fix[LogicalPlan]]) {
    def ++(that: TableContext): TableContext =
      TableContext(
        None,
        () => Fix(ObjectConcat(this.full(), that.full())),
        this.subtables ++ that.subtables)
  }

  private final case class CompilerState(
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

  type CoAnn[F[_]] = Cofree[F, Annotations]
  type CoExpr = CoAnn[ExprF]

  // CORE COMPILER
  private def compile0(node: CoExpr)(implicit M: Monad[F]):
      CompilerM[Fix[LogicalPlan]] = {
    def findFunction(name: String) =
      library.functions.find(f => f.name.toLowerCase == name.toLowerCase).fold[CompilerM[Func]](
        fail(FunctionNotFound(name)))(
        emit(_))

    def compileCases(cases: List[Case[CoExpr]], default: Fix[LogicalPlan])(f: Case[CoExpr] => CompilerM[(Fix[LogicalPlan], Fix[LogicalPlan])]) =
      cases.traverseU(f).map(_.foldRight(default) {
        case ((cond, expr), default) => Fix(relations.Cond(cond, expr, default))
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

    def flattenJoins(term: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        Fix[LogicalPlan] = relations match {
      case _: NamedRelation[_] => term
      case JoinRelation(left, right, _, _) =>
        Fix(ObjectConcat(
          flattenJoins(Fix(ObjectProject(term, LogicalPlan.Constant(Data.Str("left")))), left),
          flattenJoins(Fix(ObjectProject(term, LogicalPlan.Constant(Data.Str("right")))), right)))
    }

    def buildJoinDirectionMap(relations: SqlRelation[CoExpr]):
        Map[String, List[JoinDir]] = {
      def loop(rel: SqlRelation[CoExpr], acc: List[JoinDir]):
          Map[String, List[JoinDir]] = rel match {
        case t: NamedRelation[_] => Map(t.aliasName -> acc)
        case JoinRelation(left, right, tpe, clause) =>
          loop(left, Left :: acc) ++ loop(right, Right :: acc)
      }

      loop(relations, Nil)
    }

    def compileTableRefs(joined: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        Map[String, Fix[LogicalPlan]] =
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldRight(
            joined)(
            (dir, acc) =>
            Fix(ObjectProject(
              acc,
              LogicalPlan.Constant(Data.Str(dir.toString)))))
      }

    def tableContext(joined: Fix[LogicalPlan], relations: SqlRelation[CoExpr]):
        TableContext =
      TableContext(
        Some(joined),
        () => flattenJoins(joined, relations),
        compileTableRefs(joined, relations))

    def step(relations: SqlRelation[CoExpr]):
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

    def relationName(node: CoExpr): SemanticError \/ String = {
      val namedRel = provenanceOf(node).namedRelations
      val relations =
        if (namedRel.size <= 1) namedRel
        else {
          val filtered = namedRel.filter(x => Path(x._1).filename ≟ pprint(node.convertTo[Fix]))
          if (filtered.isEmpty) namedRel else filtered
        }
      relations.toList match {
        case Nil             => -\/ (NoTableDefined(node.convertTo[Fix]))
        case List((name, _)) =>  \/-(name)
        case x               => -\/ (AmbiguousReference(node.convertTo[Fix], x.map(_._2).join))
      }
    }

    def compileFunction(func: Func, args: List[CoExpr]):
        CompilerM[Fix[LogicalPlan]] =
      args.traverseU(compile0).map(args => Fix(func.apply(args: _*)))

    def buildRecord(names: List[Option[String]], values: List[Fix[LogicalPlan]]):
        Fix[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) =>
          Fix(MakeObject(LogicalPlan.Constant(Data.Str(name)), value))
        case (None, value) => value
      }

      // TODO: If we had an optimization pass that included eliding an
      //       ObjectConcat with an empty map on one side, this could be done
      //       in a single foldLeft.
      fields match {
        case Nil     => LogicalPlan.Constant(Data.Obj(Map()))
        case x :: xs => xs.foldLeft(x)((a, b) => Fix(ObjectConcat(a, b)))
      }
    }

    def compileRelation(r: SqlRelation[CoExpr]): CompilerM[Fix[LogicalPlan]] =
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

    node.tail match {
      case s @ SelectF(isDistinct, projections, relations, filter, groupBy, orderBy) =>
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
        val names: List[Option[String]] =
          namedProjections(node.convertTo[Fix], relationName(node).toOption.map(Path(_).filename)).map {
            case (_,    Splice(_)) => None
            case (name, _)         => Some(name)
          }
        
        val projs = projections.map(_.expr)

        val syntheticNames: List[String] =
          names.zip(syntheticOf(node)).flatMap {
            case (Some(name), Some(_)) => List(name)
            case (_,          _)       => Nil
          }

        relations.fold(
          projs.traverseU(compile0).map(buildRecord(names, _)))(
          relations => {
            val stepBuilder = step(relations)
            stepBuilder(compileRelation(relations).some) {
              val filtered = filter.map(filter =>
                (CompilerState.rootTableReq ⊛ compile0(filter))((set, filt) =>
                  Fix(Filter(set, filt))))

              stepBuilder(filtered) {
                val grouped = groupBy.map(groupBy =>
                  (CompilerState.rootTableReq ⊛
                    groupBy.keys.traverseU(compile0))((src, keys) =>
                    Fix(GroupBy(src, Fix(MakeArrayN(keys: _*))))))

                stepBuilder(grouped) {
                  val having = groupBy.flatMap(_.having).map(having =>
                    (CompilerState.rootTableReq ⊛ compile0(having))((set, filt) =>
                      Fix(Filter(set, filt))))

                  stepBuilder(having) {
                    val select =
                      (CompilerState.rootTableReq ⊛ projs.traverseU(compile0))((t, projs) =>
                        buildRecord(
                          names,
                          projs.map(p => p.unFix match {
                            case LogicalPlan.ConstantF(_) => Fix(Constantly(p, t))
                            case _                        => p
                          })))

                    val squashed = select.map(set => Fix(Squash(set)))

                    stepBuilder(squashed.some) {
                      val sort = orderBy.map(orderBy =>
                        for {
                          t <- CompilerState.rootTableReq
                          flat = names.foldMap(_.toList)
                          keys <- CompilerState.addFields(flat)(orderBy.keys.traverseU { case (_, key) => compile0(key) })
                          orders = orderBy.keys.map { case (order, _) => LogicalPlan.Constant(Data.Str(order.toString)) }
                        } yield Fix(OrderBy(t, Fix(MakeArrayN(keys: _*)), Fix(MakeArrayN(orders: _*)))))

                      stepBuilder(sort) {
                        val distincted = isDistinct match {
                          case SelectDistinct =>
                            CompilerState.rootTableReq.map(t =>
                              if (syntheticNames.nonEmpty)
                                Fix(DistinctBy(t, syntheticNames.foldLeft(t)((acc, field) =>
                                  Fix(DeleteField(acc, LogicalPlan.Constant(Data.Str(field)))))))
                              else Fix(Distinct(t))).some
                          case _ => None
                        }

                        stepBuilder(distincted) {
                          val pruned =
                            CompilerState.rootTableReq.map(
                              syntheticNames.foldLeft(_)((acc, field) =>
                                Fix(DeleteField(acc,
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

      case SetLiteralF(values0) =>
        val values = values0.map(_.tail).traverseU {
          case IntLiteralF(v) => emit[Data](Data.Int(v))
          case FloatLiteralF(v) => emit[Data](Data.Dec(v))
          case StringLiteralF(v) => emit[Data](Data.Str(v))
          case x => fail[Data](ExpectedLiteral(Fix(x.map((x: CoExpr) => x.convertTo[Fix]))))
        }

        values.map(arr => LogicalPlan.Constant(Data.Set(arr)))

      case ArrayLiteralF(exprs) =>
        exprs.traverseU(compile0).map(elems => Fix(MakeArrayN(elems: _*)))

      case SpliceF(expr) =>
        expr.fold(
          CompilerState.fullTable.flatMap(_.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find table expression for wildcard")))))(
          compile0)

      case BinopF(left, right, op) =>
        findFunction(op.name).flatMap(compileFunction(_, left :: right :: Nil))

      case UnopF(expr, op) =>
        findFunction(op.name).flatMap(compileFunction(_, expr :: Nil))

      case IdentF(name) =>
        CompilerState.fields.flatMap(fields =>
          if (fields.any(_ == name))
            CompilerState.rootTableReq.map(obj =>
              Fix(ObjectProject(obj, LogicalPlan.Constant(Data.Str(name)))))
          else
            for {
              rName   <- relationName(node).fold(fail, emit)
              table   <- CompilerState.subtableReq(rName)
            } yield
              if (Path(rName).filename ≟ name) table
              else Fix(ObjectProject(table, LogicalPlan.Constant(Data.Str(name)))))

      case InvokeFunctionF(Like.name, List(expr, pattern, escape)) =>
        (pattern.tail, escape.tail) match {
          case (StringLiteralF(str), StringLiteralF(esc)) =>
            if (esc.length > 1)
              fail(GenericError("escape character is not a single character"))
            else
              compile0(expr).map(exp =>
                Fix(Search(exp,
                  LogicalPlan.Constant(Data.Str(regexForLikePattern(str, esc.headOption))),
                  LogicalPlan.Constant(Data.Bool(false)))))
          case (x, StringLiteralF(_)) => fail(ExpectedLiteral(Fix(x.map((x: CoExpr) => x.convertTo[Fix]))))
          case (_, x)                 => fail(ExpectedLiteral(Fix(x.map((x: CoExpr) => x.convertTo[Fix]))))
        }

      case InvokeFunctionF(name, args) =>
        findFunction(name).flatMap(compileFunction(_, args))

      case MatchF(expr, cases, default0) =>
        for {
          expr    <- compile0(expr)
          default <- default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0)
          cases   <- compileCases(cases, default) {
            case Case(cse, expr2) =>
              (compile0(cse) ⊛ compile0(expr2))((cse, expr2) =>
                (Fix(relations.Eq(expr, cse)), expr2))
          }
        } yield cases

      case SwitchF(cases, default0) =>
        default0.fold(emit(LogicalPlan.Constant(Data.Null)))(compile0).flatMap(
          compileCases(cases, _) {
            case Case(cond, expr2) =>
              (compile0(cond) ⊛ compile0(expr2))((_, _))
          })

      case IntLiteralF(value) => emit(LogicalPlan.Constant(Data.Int(value)))
      case FloatLiteralF(value) => emit(LogicalPlan.Constant(Data.Dec(value)))
      case StringLiteralF(value) => emit(LogicalPlan.Constant(Data.Str(value)))
      case BoolLiteralF(value) => emit(LogicalPlan.Constant(Data.Bool(value)))
      case NullLiteralF() => emit(LogicalPlan.Constant(Data.Null))
      case VariF(name) => emit(LogicalPlan.Free(Symbol(name)))
    }
  }

  def compile(tree: Cofree[ExprF, Annotations])(implicit F: Monad[F]): F[SemanticError \/ Fix[LogicalPlan]] = {
    compile0(tree).eval(CompilerState(Nil, Nil, 0)).run.map(_.map(Compiler.reduceGroupKeys))
  }
}

object Compiler {
  import LogicalPlan._

  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def trampoline = apply[scalaz.Free.Trampoline]

  def compile(tree: Cofree[ExprF, Annotations]):
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
            Some(keys.map(_.transCataT(t => if (t ≟ src) newSrc else t)))
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
    val ann: Cofree[LogicalPlan, Boolean] = boundAttribute(tree)(keys.contains)

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
