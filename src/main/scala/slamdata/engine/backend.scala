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

sealed trait Backend {
  def dataSource: FileSystem

  def run(query: Query, out: Path): Task[(Cord, Path)]

  /**
   * Executes a query, placing the output in the specified resource, returning both
   * a compilation log and a source of values from the result set.
   */
  def eval(query: Query, out: Path): Task[(Cord, Process[Task, RenderedJson])] = {
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
  def evalLog(query: Query, out: Path): Task[Cord] = eval(query, out).map(_._1)

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a source of values from the result set.
   */
  def evalResults(query: Query, out: Path): Process[Task, RenderedJson] = Process.eval(eval(query, out).map(_._2)) flatMap identity
}

object Backend {
  private val sqlParser = new SQLParser()

  def apply[PhysicalPlan: Show, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: FileSystem, showNative: PhysicalPlan => Cord) = new Backend {
    private type ProcessTask[A] = Process[Task, A]

    private type WriterCord[A] = Writer[Cord, A]

    private type EitherError[A] = Error \/ A

    private type EitherWriter[A] = EitherT[WriterCord, Error, A]

    private def logged[A: Show](caption: String)(ea: Error \/ A): EitherWriter[A] = {
      var log0 = Cord(caption)

      val log = ea.fold(
        error => log0 ++ Cord(error.fullMessage),
        a     => log0 ++ a.show
      )

      EitherT[WriterCord, Error, A](WriterT.writer[Cord, Error \/ A](log, ea))
    }

    private def logged(caption: String, phys: PhysicalPlan): EitherWriter[PhysicalPlan] = {
      val log = Cord(caption) ++ showNative(phys)
      EitherT[WriterCord, Error, PhysicalPlan](WriterT.writer[Cord, Error \/ PhysicalPlan](log, \/- (phys)))
    }

    def dataSource = ds

    def run(query: Query, out: Path): Task[(Cord, Path)] = Task.delay {
      import SemanticAnalysis.{fail => _, _}
      import Process.{logged => _, _}

      def loggedTask[A](log: Cord, t: Task[A]): Task[(Cord, A)] = 
        new Task(t.get.map(_.bimap({
          case e : Error => LoggedError(log, e)
          case e => e
          },
          log -> _)))

      val either = for {
        select     <- logged("\nSQL AST\n")(sqlParser.parse(query))
        tree       <- logged("\nAnnotated Tree\n")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
        logical    <- logged("\nLogical Plan\n")(Compiler.compile(tree))
        simplified <- logged("\nSimplified\n")(\/-(Optimizer.simplify(logical)))
        physical   <- logged("\nPhysical Plan\n")(planner.plan(simplified))
        _          <- logged("\nMongo\n", physical)
      } yield physical

      val (log, physical) = either.run.run

      physical.fold[Task[(Cord, Path)]](
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
