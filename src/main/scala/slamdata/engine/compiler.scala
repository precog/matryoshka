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

  private case class CompilerState(tree: AnnotatedTree[Node, Ann], tableMap: Map[String, Term[LogicalPlan2]] = Map.empty[String, Term[LogicalPlan2]])

  private object CompilerState {
    def addTable(name: String, plan: Term[LogicalPlan2])(implicit m: Monad[F]) = 
      mod((s: CompilerState) => s.copy(tableMap = s.tableMap + (name -> plan)))

    def getTable(name: String)(implicit m: Monad[F]) = 
      read[CompilerState, Term[LogicalPlan2]](_.tableMap(name))
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

  private def invoke(func: Func, args: List[Node])(implicit m: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan2]] = for {
    args <- args.map(compile0).sequenceU
    rez  <- emit(LogicalPlan2.invoke(func, args))
  } yield rez

  // CORE COMPILER
  private def compile0(node: Node)(implicit M: Monad[F]): StateT[M, CompilerState, Term[LogicalPlan2]] = {
    def optInvoke2[A <: Node](default: Term[LogicalPlan2], option: Option[A])(func: Func) = {
      option.map(compile0).map(_.map(c => LogicalPlan2.invoke(func, default :: c :: Nil))).getOrElse(emit(default))
    }

    def compileCases(cases: List[Case], default: Node)(f: Case => CompilerM[(Term[LogicalPlan2], Term[LogicalPlan2])]) = {
     for {
        cases   <- cases.map(f).sequenceU
        default <- compile0(default)
      } yield cases.foldRight(default) { case ((cond, expr), default) => 
        LogicalPlan2.invoke(relations.Cond, cond :: expr :: default :: Nil) 
      }
    }

    def find1Ident(expr: Expr): StateT[M, CompilerState, Ident] = {
      val tree = Tree[Node](expr, _.children)

      (tree.collect {
        case x @ Ident(_) => x
      }) match {
        case one :: Nil => emit(one)
        case _ => fail(ExpectedOneTableInJoin(expr))
      }
    }

    def relationName(node: Node): StateT[M, CompilerState, String] = {
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

    def compileJoin(clause: Expr): StateT[M, CompilerState, (LogicalPlan2.JoinRel, Term[LogicalPlan2], Term[LogicalPlan2])] = {
      def compileJoinSide(side: Expr, sideName: String): StateT[M, CompilerState, Term[LogicalPlan2]] = whatif(for {
        ident <- find1Ident(side)
        name  <- relationName(ident)
        _     <- CompilerState.addTable(name, LogicalPlan2.read(name))
        cmp   <- compile0(side)
      } yield cmp)

      clause match {
        case InvokeFunction(f, left :: right :: Nil) =>
          val joinRel = 
            if (f == relations.Eq) emit(LogicalPlan2.JoinRel.Eq)
            else if (f == relations.Lt) emit(LogicalPlan2.JoinRel.Lt)
            else if (f == relations.Gt) emit(LogicalPlan2.JoinRel.Gt)
            else if (f == relations.Lte) emit(LogicalPlan2.JoinRel.Lte)
            else if (f == relations.Gte) emit(LogicalPlan2.JoinRel.Gte)
            else if (f == relations.Neq) emit(LogicalPlan2.JoinRel.Neq)
            else fail(UnsupportedJoinCondition(clause))

          // FIXME: FRESH NAMES!!!!
          for {
            rel   <- joinRel
            left  <- compileJoinSide(left, "left")
            right <- compileJoinSide(right, "right")
            rez   <- emit((rel, left, right))
          } yield rez

        case _ => fail(UnsupportedJoinCondition(clause))
      }
    }

    def compileFunction(func: Func, args: List[Expr]): StateT[M, CompilerState, Term[LogicalPlan2]] = {
      import structural.ArrayProject

      // TODO: Make this a desugaring pass once AST transformations are supported

      val specialized: PartialFunction[(Func, List[Expr]), StateT[M, CompilerState, Term[LogicalPlan2]]] = {
        case (`ArrayProject`, arry :: Wildcard :: Nil) => compileFunction(structural.FlattenArray, arry :: Nil)
      }

      val default: (Func, List[Expr]) => StateT[M, CompilerState, Term[LogicalPlan2]] = { (func, args) =>
        for {
          args <- args.map(compile0).sequenceU
        } yield LogicalPlan2.invoke(func, args)
      }

      specialized.applyOrElse((func, args), default.tupled)
    }

    node match {
      case s @ SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
        val (names0, projs) = s.namedProjections.unzip

        val names = names0.map(name => LogicalPlan2.constant(Data.Str(name)): Term[LogicalPlan2])

        // We compile the relations first thing, because they will add the tables to the CompilerState 
        // table map, which is necessary for compiling the projections (which look in the map to link
        // to the compiled tables).

        for {
          relations <-  relations.map(compile0).sequenceU
          crossed   <-  Foldable[List].foldLeftM[CompilerM, Term[LogicalPlan2], Term[LogicalPlan2]](
                          relations.tail, relations.head
                        )((left, right) => emit[Term[LogicalPlan2]](LogicalPlan2.invoke(Cross, left :: right :: Nil)))
          filter    <-  optInvoke2(crossed, filter)(Filter)
          groupBy   <-  optInvoke2(filter, groupBy)(GroupBy)
          offset    <-  optInvoke2(groupBy, offset.map(IntLiteral.apply _))(Drop)
          limit     <-  optInvoke2(offset, limit.map(IntLiteral.apply _))(Take)
          projs     <-  projs.map(compile0).sequenceU
        } yield {
          val fields = names.zip(projs).map {
            case (name, proj) => LogicalPlan2.invoke(MakeObject, name :: proj :: Nil): Term[LogicalPlan2]
          }

          fields.reduce((a, b) => LogicalPlan2.invoke(ObjectConcat, a :: b :: Nil))
        }

      case Subselect(select) => compile0(select)

      case SetLiteral(values0) => 
        val values = (values0.map { 
          case IntLiteral(v) => emit[Data](Data.Int(v))
          case FloatLiteral(v) => emit[Data](Data.Dec(v))
          case StringLiteral(v) => emit[Data](Data.Str(v))
          case x => fail[Data](ExpectedLiteral(x))
        }).sequenceU

        values.map((Data.Set.apply _) andThen (LogicalPlan2.constant _))

      case Wildcard =>
        // Except when it appears as the argument to ARRAY_PROJECT, wildcard
        // always means read everything from the table:
        for {
          name <- relationName(node)
        } yield LogicalPlan2.read(name)

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

      case ident @ Ident(name) => 
        for {
          prov <- provenanceOf(node)
          name <- relationName(ident)
          plan <- CompilerState.getTable(name)
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
                        } yield (LogicalPlan2.invoke(relations.Eq, expr :: cse :: Nil), expr2) 
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

      case IntLiteral(value) => emit(LogicalPlan2.constant(Data.Int(value)))

      case FloatLiteral(value) => emit(LogicalPlan2.constant(Data.Dec(value)))

      case StringLiteral(value) => emit(LogicalPlan2.constant(Data.Str(value)))

      case NullLiteral() => emit(LogicalPlan2.constant(Data.Null))

      case t @ TableRelationAST(name, alias) => 
        for {
          value <- emit(LogicalPlan2.read(name))
          _     <- CompilerState.addTable(t.aliasName, value)
        } yield value

      case t @ SubqueryRelationAST(subquery, alias) => 
        for {
          subquery <- compile0(subquery)
          _        <- CompilerState.addTable(t.aliasName, subquery)
        } yield subquery

      case JoinRelation(left, right, tpe, clause) => 
        for {
          left   <- compile0(left)
          right  <- compile0(right)
          tuple  <- compileJoin(clause)
          rez    <- emit(
                      LogicalPlan2.join(left, right, tpe match {
                        case LeftJoin  => LogicalPlan2.JoinType.LeftOuter
                        case InnerJoin => LogicalPlan2.JoinType.Inner
                        case RightJoin => LogicalPlan2.JoinType.RightOuter
                        case FullJoin  => LogicalPlan2.JoinType.FullOuter
                      }, tuple._1, tuple._2, tuple._3)
                    )
        } yield rez

      case CrossRelation(left, right) =>
        for {
          left  <- compile0(left)
          right <- compile0(right)
          rez   <- emit(LogicalPlan2.invoke(Cross, left :: right :: Nil))
        } yield rez

      case _ => fail(NonCompilableNode(node))
    }
  }

  def compile(tree: AnnotatedTree[Node, Ann])(implicit F: Monad[F]): F[SemanticError \/ Term[LogicalPlan2]] = {
    compile0(tree.root).eval(CompilerState(tree)).run
  }
}

object Compiler {
  type Ann = ((Type, Option[Func]), Provenance)

  def apply[F[_]]: Compiler[F] = new Compiler[F] {}

  def id = apply[Id.Id]

  def trampoline = apply[Free.Trampoline]

  def compile(tree: AnnotatedTree[Node, Ann]): SemanticError \/ Term[LogicalPlan2] = {
    trampoline.compile(tree).run
  }
}