package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.sql._
import slamdata.engine.analysis.fixplate._

import SemanticAnalysis._
import SemanticError._
import slamdata.engine.std.StdLib._

import scalaz.{Id, Free, Monad, EitherT, StateT, IndexedStateT, Applicative, \/, Foldable}

import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.syntax.applicative._

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

  private case class TableContext(root: Term[LogicalPlan], subtables: Map[String, Term[LogicalPlan]])

  private case class CompilerState(
    tree:         AnnotatedTree[Node, Ann], 
    subtables:    Map[String, Term[LogicalPlan]] = Map.empty[String, Term[LogicalPlan]],
    tableContext: List[TableContext] = Nil,
    nameGen:      Int = 0
  )

  private object CompilerState {
    /**
     * Runs a computation inside a table context, which contains compilation data
     * for the tables in scope.
     */
    def contextual[A](t: TableContext)(f: CompilerM[A])(implicit m: Monad[F]): CompilerM[A] = for {
      _ <- mod((s: CompilerState) => s.copy(tableContext = t :: s.tableContext))
      a <- f
      _ <- mod((s: CompilerState) => s.copy(tableContext = s.tableContext.tail))
    } yield a

    def rootTable(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] = 
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.map(_.root))

    def subtable(name: String)(implicit m: Monad[F]): CompilerM[Option[Term[LogicalPlan]]] = 
      read[CompilerState, Option[Term[LogicalPlan]]](_.tableContext.headOption.flatMap(_.subtables.get(name)))

    /**
     * Generates a fresh name for use as an identifier, e.g. tmp321.
     */
    def freshName(implicit m: Monad[F]): CompilerM[Symbol] = for {
      num <- read[CompilerState, Int](_.nameGen)
      _   <- mod((s: CompilerState) => s.copy(nameGen = s.nameGen + 1))
    } yield Symbol("tmp" + num.toString)
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

  private def invoke(func: Func, args: List[Node])(implicit m: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan]] = for {
    args <- args.map(compile0).sequenceU
    rez  <- emit(LogicalPlan.invoke(func, args))
  } yield rez

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

    def buildJoinDirectionMap(relations: List[SqlRelation]): Map[String, List[JoinTraverse.Dir]] = {
      def loop(rel: SqlRelation): JoinTraverse = rel match {
        case t @ TableRelationAST(name, aliasOpt) => JoinTraverse.Leaf(t.aliasName)
        case t @ SubqueryRelationAST(subquery, alias) => JoinTraverse.Leaf(t.aliasName) // Leaf????
        case JoinRelation(left, right, tpe, clause) => JoinTraverse.Join(loop(left), loop(right))
        case CrossRelation(left, right) => JoinTraverse.Join(loop(left), loop(right))
      }

      (relations.map(loop _).foldLeft[Option[JoinTraverse]](None) {
        case (None, traverse) => Some(traverse)
        case (Some(acc), traverse) => Some(JoinTraverse.Join(acc, traverse))
      }).map(_.directionMap).getOrElse(Map())
    }

    def compileTableRefs(joined: Term[LogicalPlan], relations: List[SqlRelation]): Map[String, Term[LogicalPlan]] = {
      buildJoinDirectionMap(relations).map {
        case (name, dirs) =>
          name -> dirs.foldLeft(joined) {
            case (acc, dir) =>
              val dirName = if (dir == JoinTraverse.Left) "left" else "right"

              LogicalPlan.invoke(ObjectProject, acc :: LogicalPlan.constant(Data.Str(dirName)) :: Nil)
          }
      }
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

        val relations = prov.namedRelations

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

        case _ => fail(UnsupportedJoinCondition(clause))
      }
    }

    def compileFunction(func: Func, args: List[Expr]): StateT[M, CompilerState, Term[LogicalPlan]] = {
      // TODO: Make this a desugaring pass once AST transformations are supported
      val specialized: PartialFunction[(Func, List[Expr]), StateT[M, CompilerState, Term[LogicalPlan]]] = {
        case (`ArrayProject`, arry :: Wildcard :: Nil) => compileFunction(structural.FlattenArray, arry :: Nil)
      }

      val default: (Func, List[Expr]) => StateT[M, CompilerState, Term[LogicalPlan]] = { (func, args) =>
        for {
          args <- args.map(compile0).sequenceU
        } yield LogicalPlan.invoke(func, args)
      }

      specialized.applyOrElse((func, args), default.tupled)
    }

    def buildSelectRecord(names: List[Option[Term[LogicalPlan]]], values: List[Term[LogicalPlan]]): Term[LogicalPlan] = {
      val fields = names.zip(values).map {
        case (Some(name), value) => LogicalPlan.invoke(MakeObject, name :: value :: Nil): Term[LogicalPlan]
        case (None, value) => value
      }

      fields.reduce((a, b) => LogicalPlan.invoke(ObjectConcat, a :: b :: Nil)) 
    }

    node match {
      case s @ SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
        val namedProjs = s.namedProjections

        val (_, projs) = namedProjs.unzip

        // Wildcards don't have names, we merge them into any other objects created from other columns:
        val names = namedProjs.map {
          case (name, Wildcard) => None
          case (name, value)    => Some(LogicalPlan.constant(Data.Str(name)))
        }

        if (relations.length == 0) for {
          projs <- projs.map(compile0).sequenceU
        } yield buildSelectRecord(names, projs)
        else for {
          joined    <-  relations.map(compile0).sequenceU
          crossed   <-  Foldable[List].foldLeftM[CompilerM, Term[LogicalPlan], Term[LogicalPlan]](
                          joined.tail, joined.head
                        )((left, right) => emit[Term[LogicalPlan]](LogicalPlan.invoke(Cross, left :: right :: Nil)))
          filtered  <-  optInvoke2(crossed, filter)(Filter)
          grouped   <-  optInvoke2(filtered, groupBy)(GroupBy)
          sorted    <-  optInvoke2(grouped, orderBy)(OrderBy)
          skipped   <-  optInvoke2(sorted, offset.map(IntLiteral.apply _))(Drop)
          limited   <-  optInvoke2(skipped, limit.map(IntLiteral.apply _))(Take)

          joinName  <- CompilerState.freshName
          joinedRef <- emit(LogicalPlan.free(joinName))

          projs     <-  CompilerState.contextual(TableContext(joinedRef, compileTableRefs(joinedRef, relations)))(projs.map(compile0).sequenceU)
        } yield LogicalPlan.let(Map(joinName -> limited), buildSelectRecord(names, projs))

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
          tableOpt <- CompilerState.rootTable
          table    <- tableOpt.map(emit _).getOrElse(fail(GenericError("Not within a table context so could not find root table for wildcard")))
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
          tableOpt  <-  CompilerState.subtable(name)
          table     <-  tableOpt.map(emit _).getOrElse(fail(GenericError("Could not find compiled plan for table " + name)))
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

      case t @ TableRelationAST(name, alias) => 
        for {
          value <- emit(LogicalPlan.read(name))
        } yield value

      case t @ SubqueryRelationAST(subquery, alias) => 
        for {
          subquery <- compile0(subquery)
        } yield subquery

      case JoinRelation(left, right, tpe, clause) => 
        for {
          left   <- compile0(left)
          right  <- compile0(right)
          tuple  <- compileJoin(clause)
          rez    <- emit(
                      LogicalPlan.join(left, right, tpe match {
                        case LeftJoin  => LogicalPlan.JoinType.LeftOuter
                        case InnerJoin => LogicalPlan.JoinType.Inner
                        case RightJoin => LogicalPlan.JoinType.RightOuter
                        case FullJoin  => LogicalPlan.JoinType.FullOuter
                      }, tuple._1, tuple._2, tuple._3)
                    )
        } yield rez

      case CrossRelation(left, right) =>
        for {
          left  <- compile0(left)
          right <- compile0(right)
          rez   <- emit(LogicalPlan.invoke(Cross, left :: right :: Nil))
        } yield rez

      case sql.GroupBy(keys, _) => // TODO: "having"
        for {
          keys <- keys.map(compile0).sequenceU
          val arrays = keys.map(k => LogicalPlan.invoke(MakeArray, k :: Nil))
          val groupKey = arrays.foldLeft(LogicalPlan.constant(Data.Int(1))) {
            case (acc, arr) => LogicalPlan.invoke(ArrayConcat, acc :: arr :: Nil)
          }
        } yield LogicalPlan.invoke(GroupBy, ??? :: groupKey :: Nil)
        

      case sql.OrderBy(names) =>
        ???
//        names.map {
//          case (expr, oType) => compile0(expr)
//        }
        
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