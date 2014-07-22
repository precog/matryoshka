package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.std._
import slamdata.engine.sql._
import slamdata.engine.analysis._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.physical.mongodb._
import slamdata.engine.fs._

import scalaz.{Node => _, Tree => _, _}
import scalaz.concurrent.{Node => _, _}
import Scalaz._

import scalaz.stream.{Writer => _, _}

import slamdata.engine.config._

// FIXME: better name?
sealed trait PhaseResult {
  def name: String
}
object PhaseResult {
  import slamdata.engine.{Error => SDError}
  case class Error(name: String, value: SDError) extends PhaseResult
  case class Tree(name: String, value: RenderedTree) extends PhaseResult {
    override def toString = name + "\n" + Show[RenderedTree].shows(value)
  }
  case class Detail(name: String, value: String) extends PhaseResult {
    override def toString = name + "\n" + value
  }
}

sealed trait Backend {
  def dataSource: FileSystem

  def run(query: Query, out: Path): Task[(Seq[PhaseResult], Path)]

  /**
   * Executes a query, placing the output in the specified resource, returning both
   * a compilation log and a source of values from the result set.
   */
  def eval(query: Query, out: Path): Task[(Seq[PhaseResult], Process[Task, RenderedJson])] = {
    for {
      db    <- dataSource.delete(out)
      t     <- run(query, out)

      (log, out) = t

      proc  <- Task.delay(dataSource.scanAll(out))
    } yield log -> proc
  }

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a compilation log.
   */
  def evalLog(query: Query, out: Path): Task[Seq[PhaseResult]] = eval(query, out).map(_._1)

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a source of values from the result set.
   */
  def evalResults(query: Query, out: Path): Process[Task, RenderedJson] = Process.eval(eval(query, out).map(_._2)) flatMap identity
}

object Backend {
  private val sqlParser = new SQLParser()

  def apply[PhysicalPlan: RenderTree, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: FileSystem, showNative: PhysicalPlan => Cord) = new Backend {
    private type ProcessTask[A] = Process[Task, A]

    private type WriterResult[A] = Writer[Vector[PhaseResult], A]

    private type EitherWriter[A] = EitherT[WriterResult, Error, A]

    private def withTree[A](name: String)(ea: Error \/ A)(implicit RA: RenderTree[A]): EitherWriter[A] = {
      val result = ea.fold(
        error => PhaseResult.Error(name, error),
        a     => PhaseResult.Tree(name, RA.render(a))
      )

      EitherT[WriterResult, Error, A](WriterT.writer[Vector[PhaseResult], Error \/ A]((Vector.empty :+ result) -> ea))
    }

    private def withString[A](name: String)(a: A)(render: A => Cord): EitherWriter[A] = {
      val result = PhaseResult.Detail(name, render(a).toString)

      EitherT[WriterResult, Error, A](
        WriterT.writer[Vector[PhaseResult], Error \/ A](
          (Vector.empty :+ result) -> \/- (a)))
    }

    def dataSource = ds

    def run(query: Query, out: Path): Task[(Seq[PhaseResult], Path)] = Task.delay {
      import SemanticAnalysis.{fail => _, _}
      import Process.{logged => _, _}

      def loggedTask[A](log: Seq[PhaseResult], t: Task[A]): Task[(Seq[PhaseResult], A)] = 
        new Task(t.get.map(_.bimap({
          case e : Error => LoggedError(log, e)
          case e => e
          },
          log -> _)))

      val either = for {
        select     <- withTree("SQL AST")(sqlParser.parse(query))
        tree       <- withTree("Annotated Tree")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
        logical    <- withTree("Logical Plan")(Compiler.compile(tree))
        simplified <- withTree("Simplified")(\/-(Optimizer.simplify(logical)))
        physical   <- withTree("Physical Plan")(planner.plan(simplified))
        _          <- withString("Mongo")(physical)(showNative)
      } yield physical

      val (log, physical) = either.run.run

      physical.fold[Task[(Seq[PhaseResult], Path)]](
        error => Task.fail(LoggedError(log, error)),
        plan => loggedTask(log, evaluator.execute(plan, out))
      )
    }.join
  }
}

case class BackendDefinition(create: PartialFunction[BackendConfig, Task[Backend]]) extends (BackendConfig => Option[Task[Backend]]) {
  def apply(config: BackendConfig): Option[Task[Backend]] = create.lift(config)
}

object BackendDefinition {
  implicit val BackendDefinitionMonoid = new Monoid[BackendDefinition] {
    def zero = BackendDefinition(PartialFunction.empty)

    def append(v1: BackendDefinition, v2: => BackendDefinition): BackendDefinition = 
      BackendDefinition(v1.create.orElse(v2.create))
  }
}
