package slamdata.engine

import slamdata.engine.std._
import slamdata.engine.sql._
import slamdata.engine.analysis._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.physical.mongodb._

import scalaz.Monoid
import scalaz.concurrent.{Node => _, _}
import scalaz.{NonEmptyList, Show}
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.syntax._

import scalaz.stream._

import slamdata.engine.config._

sealed trait Backend {
  def execute(query: String, out: String): Process[Task, RenderedJson]
}

object Backend {
  private val sqlParser = new SQLParser()
  def apply[PhysicalPlan, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: DataSource) = new Backend {
    def execute(query: String, out: String): Process[Task, RenderedJson] = {
      import SemanticAnalysis.{fail => _, _}
      import Process._

      sqlParser.parse(query).fold(
        fail _,
        select => {
          AllPhases(tree(select)).fold(
            errors => fail(ManyErrors(errors)),
            tree => {
              Compiler.compile(tree).fold(
                fail _,
                logical => {
                  planner.plan(logical).fold(
                    fail _,
                    logical => {
                      eval(for {
                        db   <- ds.delete(out)
                        _    <- evaluator.execute(logical, out)
                        proc <- Task.delay(ds.scan(out))
                      } yield proc).flatMap(identity _)
                    }
                  )
                }
              )
            }
          )
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