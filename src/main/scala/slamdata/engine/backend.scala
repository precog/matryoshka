package slamdata.engine

import slamdata.engine.std._
import slamdata.engine.sql._
import slamdata.engine.analysis._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.physical.mongodb._

import scalaz.{Node => _, Tree => _, _}
import scalaz.concurrent.{Node => _, _}
import Scalaz._

import scalaz.stream.{Writer => _, _}

import slamdata.engine.config._

sealed trait Backend {
  // TODO: newtype query & out
  def execute(query: String, out: String): Task[(Cord, Process[Task, RenderedJson])]
}

object Backend {
  private val sqlParser = new SQLParser()
  
  def apply[PhysicalPlan: Show, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: DataSource) = new Backend {
    private type ProcessTask[A] = Process[Task, A]

    private type WriterCord[A] = Writer[Cord, A]

    private type EitherError[A] = Error \/ A

    private type EitherWriter[A] = EitherT[WriterCord, Error, A]

    private def logged[A: Show](caption: String)(ea: Error \/ A): EitherWriter[A] = {
      val log = ea.fold(
        error => Cord(error.fullMessage),
        a     => Cord(caption) ++ a.show
      )

      EitherT[WriterCord, Error, A](WriterT.writer[Cord, Error \/ A](log, ea))
    }

    def execute(query: String, out: String): Task[(Cord, Process[Task, RenderedJson])] = Task.delay {
      import SemanticAnalysis.{fail => _, _}
      import Process.{logged => _, _}

      val either = for {
        select    <- logged("\nSQL AST\n")(sqlParser.parse(query))
        tree      <- logged("\nAnnotated Tree\n")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
        logical   <- logged("\nLogical Plan\n")(Compiler.compile(tree))
        physical  <- logged("\nPhysical Plan\n")(planner.plan(logical))
      } yield physical

      val (log, physical) = either.run.run

      (physical: Error \/ PhysicalPlan).fold(
        error => (log, fail(error)),
        logical => {
          log -> eval(for {
            db   <- ds.delete(out)
            _    <- evaluator.execute(logical, out)
            proc <- Task.delay(ds.scan(out))
          } yield proc).flatMap(identity _)
        }
      )
    }
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