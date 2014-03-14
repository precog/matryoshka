package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.sql._

import SemanticAnalysis._
import SemanticError._
import slamdata.engine.std.StdLib._

import scalaz.{Monad, EitherT, StateT, IndexedStateT, Applicative, \/, Foldable}

import scalaz.std.list._
import scalaz.syntax.traverse._

trait Compiler {
  protected type F[_]

  protected implicit def MonadF: Monad[F]

  def readFromTable(name: String): LogicalPlan

  // ANNOTATIONS
  private type Ann = ((Type, Option[Func]), Provenance)

  private def typeOf(node: Node): StateT[M, CompilerState, Type] = attr(node).map(_._1._1)

  private def provenance(node: Node): StateT[M, CompilerState, Provenance] = attr(node).map(_._2)

  private def funcOf(node: Node): StateT[M, CompilerState, Func] = for {
    funcOpt <- attr(node).map(_._1._2)
    rez     <- funcOpt.map(emit _).getOrElse(fail(FunctionNotBound(node)))
  } yield rez

  // HELPERS
  private type M[A] = EitherT[F, SemanticError, A]

  private implicit val MonadM = implicitly[Monad[M]]

  private type State[A] = StateT[M, CompilerState, A]

  private implicit val MonadState = implicitly[Monad[State]]

  private case class CompilerState(tree: AnnotatedTree[Node, Ann], tableMap: Map[String, LogicalPlan] = Map.empty[String, LogicalPlan])

  private object CompilerState {
    def addTable(name: String, plan: LogicalPlan) = mod((s: CompilerState) => s.copy(tableMap = s.tableMap + (name -> plan)))
  }

  private def read[A, B](f: A => B): StateT[M, A, B] = StateT((s: A) => Applicative[M].point((s, f(s))))

  private def attr(node: Node): StateT[M, CompilerState, Ann] = read(s => s.tree.attr(node))

  private def tree: StateT[M, CompilerState, AnnotatedTree[Node, Ann]] = read(s => s.tree)

  private def fail[A](error: SemanticError): StateT[M, CompilerState, A] = {
    StateT[M, CompilerState, A]((s: CompilerState) => EitherT.eitherT(Applicative[F].point(\/.left(error))))
  }

  private def emit[A](value: A): StateT[M, CompilerState, A] = {
    StateT[M, CompilerState, A]((s: CompilerState) => EitherT.eitherT(Applicative[F].point(\/.right(s -> value))))
  }

  private def mod(f: CompilerState => CompilerState): StateT[M, CompilerState, Unit] = StateT[M, CompilerState, Unit](s => Applicative[M].point(f(s) -> Unit))

  private def invoke(func: Func, args: List[Node]): StateT[M, CompilerState, LogicalPlan] = for {
    args <- args.map(compile0).sequenceU
    rez  <- emit(LogicalPlan.Invoke(func, args))
  } yield rez

  private def optCompile[A <: Node](default: LogicalPlan, option: Option[A])(f: (LogicalPlan, LogicalPlan) => LogicalPlan) = {
    option.map(compile0).map(_.map(c => f(default, c))).getOrElse(emit(default))
  }

  private def opt[A](default: LogicalPlan, option: Option[A])(f: (LogicalPlan, A) => LogicalPlan) = {
    emit(option.map(c => f(default, c)).getOrElse(default))
  }

  // CORE COMPILER
  private def compile0(node: Node): StateT[M, CompilerState, LogicalPlan] = node match {
    case s @ SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
      val (names, projs) = s.namedProjections.unzip

      for {
        relations <-  relations.map(compile0).sequenceU
        crossed   <-  Foldable[List].foldLeftM[State, LogicalPlan, LogicalPlan](
                        relations.tail, relations.head
                      )((left, right) => emit[LogicalPlan](LogicalPlan.Cross(left, right)))
        filter    <-  optCompile(crossed, filter)(LogicalPlan.Filter.apply _)
        groupBy   <-  optCompile(filter, groupBy)(LogicalPlan.Group.apply _)
        offset    <-  opt(groupBy, offset)(LogicalPlan.Drop.apply _)
        limit     <-  opt(offset, limit)(LogicalPlan.Take.apply _)
      } yield limit

    case Subselect(select) => compile0(select)

    case SetLiteral(values0) => 
      val values = (values0.map { 
        case IntLiteral(v) => emit[Data](Data.Int(v))
        case FloatLiteral(v) => emit[Data](Data.Dec(v))
        case StringLiteral(v) => emit[Data](Data.Str(v))
        case x => fail[Data](ExpectedLiteral(x))
      }).sequenceU

      values.map((Data.Set.apply _) andThen (LogicalPlan.Constant.apply _))

    case Wildcard =>
      ???

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
      ???

    case InvokeFunction(name, args) => 
      for {
        args <- args.toList.map(compile0).sequenceU
        func <- funcOf(node)
        rez  <- emit(LogicalPlan.Invoke(func, args))
      } yield rez

    case Case(cond, expr) => 
      ???

    case Match(expr, cases, default) => 
      ???

    case Switch(cases, default) => 
      ???

    case IntLiteral(value) => emit(LogicalPlan.Constant(Data.Int(value)))

    case FloatLiteral(value) => emit(LogicalPlan.Constant(Data.Dec(value)))

    case StringLiteral(value) => emit(LogicalPlan.Constant(Data.Str(value)))

    case NullLiteral() => emit(LogicalPlan.Constant(Data.Null))

    case TableRelationAST(name, alias) => 
      for {
        value <- emit(readFromTable(name))
        _     <- CompilerState.addTable(alias.getOrElse(name), value)
      } yield value

    case SubqueryRelationAST(subquery, alias) => 
      for {
        subquery <- compile0(subquery)
        _        <- CompilerState.addTable(alias, subquery)
      } yield subquery

    case JoinRelation(left, right, tpe, clause) => 
      for {
        left   <- compile0(left)
        right  <- compile0(right)
        clause <- compile0(clause)
        rez    <- emit(
                    LogicalPlan.Join(left, right, tpe match {
                      case LeftJoin  => LogicalPlan.LeftOuter
                      case InnerJoin => LogicalPlan.Inner
                      case RightJoin => LogicalPlan.RightOuter
                      case FullJoin  => LogicalPlan.FullOuter
                    }, ???, ???) // TODO
                  )
      } yield rez

    case CrossRelation(left, right) =>
      for {
        left  <- compile0(left)
        right <- compile0(right)
        rez   <- emit(LogicalPlan.Cross(left, right))
      } yield rez

    case _ => fail(NonCompilableNode(node))
  }

  def compile(tree: AnnotatedTree[Node, Ann]): F[SemanticError \/ LogicalPlan] = {
    compile0(tree.root).eval(CompilerState(tree)).run
  }
}