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

sealed trait PhaseResult {
  def name: String
}
object PhaseResult {
  import argonaut._
  import Argonaut._

  import slamdata.engine.{Error => SDError}

  case class Error(name: String, value: SDError) extends PhaseResult {
    override def toString = name + "\n" + value.toString
  }
  case class Tree(name: String, value: RenderedTree) extends PhaseResult {
    override def toString = name + "\n" + Show[RenderedTree].shows(value)
  }
  case class Detail(name: String, value: String) extends PhaseResult {
    override def toString = name + "\n" + value
  }

  implicit def PhaseResultEncodeJson: EncodeJson[PhaseResult] = EncodeJson {
    case PhaseResult.Error(name, value) =>
      Json.obj(
        "name"  := name,
        "error" := value.getMessage
      )
    case PhaseResult.Tree(name, value) =>
      Json.obj(
        "name" := name,
        "tree" := value
      )
    case PhaseResult.Detail(name, value) =>
      Json.obj(
        "name"   := name,
        "detail" := value
      )
  }
}

sealed trait Backend {
  def dataSource: FileSystem

  /**
   * Executes a query, producing a compilation log and the path where the result
   * can be found.
   */
  def run(req: QueryRequest): (Vector[PhaseResult], Task[ResultPath])

  /**
   * Executes a query, placing the output in the specified resource, returning both
   * a compilation log and a source of values from the result set.
   */
  def eval(req: QueryRequest): (Vector[PhaseResult], Task[Process[Task, RenderedJson]]) = {
    val (log, outT) = run(req)
    log -> (for {
      _   <- req.out.map(dataSource.delete(_)).getOrElse(Task.now(()))
      out <- outT
    } yield {
      val results = dataSource.scanAll(out.path)

      out match {
        case ResultPath.Temp(path) => results.cleanUpWith(dataSource.delete(path))
        case _ => results
      }
    })
  }

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a compilation log.
   */
  def evalLog(req: QueryRequest): Vector[PhaseResult] = eval(req)._1

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a source of values from the result set.
   */
  def evalResults(req: QueryRequest): Process[Task, RenderedJson] = Process.eval(eval(req)._2) flatMap identity
}

object Backend {
  def apply[PhysicalPlan: RenderTree, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: FileSystem, showNative: PhysicalPlan => Cord) = new Backend {
    def dataSource = ds

    val queryPlanner = planner.queryPlanner(showNative)

    def run(req: QueryRequest): (Vector[PhaseResult], Task[ResultPath]) = {
      import Process.{logged => _, _}

      def loggedTask[A](log: Vector[PhaseResult], t: Task[A]): Task[(Vector[PhaseResult], A)] =
        new Task(t.get.map(_.bimap(
          {
            case e : Error => PhaseError(log :+ PhaseResult.Error("Execution", e), e)
            case e => e
          },
          log -> _)))

      val (phases, physical) = queryPlanner(req)
      
      phases -> 
        physical.fold[Task[ResultPath]](
          error => Task.fail(PhaseError(phases, error)),
          plan => for {
            rez     <- evaluator.execute(plan)
            renamed <- (rez, req.out) match {
              case (ResultPath.Temp(path), Some(out)) => for {
                  _ <- dataSource.move(path, out)
                } yield ResultPath.User(out)
              case _ => Task.now(rez)
            }
          } yield renamed)
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
