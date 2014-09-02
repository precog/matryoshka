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

  case class Error(name: String, value: SDError) extends PhaseResult
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

case class QueryRequest(query: Query, mountPath: Path, basePath: Path, out: Path)

sealed trait Backend {
  def dataSource: FileSystem

  /**
   * Executes a query, producing a compilation log and the path where the result
   * can be found.
   */
  def run(req: QueryRequest): Task[(Vector[PhaseResult], Path)]

  /**
   * Executes a query, placing the output in the specified resource, returning both
   * a compilation log and a source of values from the result set.
   */
  def eval(req: QueryRequest): Task[(Vector[PhaseResult], Process[Task, RenderedJson])] = {
    for {
      _     <- dataSource.delete(req.out)
      t     <- run(req)

      (log, out) = t

      proc  <- Task.delay(dataSource.scanAll(out))
    } yield log -> proc
  }

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a compilation log.
   */
  def evalLog(req: QueryRequest): Task[Vector[PhaseResult]] = eval(req).map(_._1)

  /**
   * Executes a query, placing the output in the specified resource, returning only
   * a source of values from the result set.
   */
  def evalResults(req: QueryRequest): Process[Task, RenderedJson] = Process.eval(eval(req).map(_._2)) flatMap identity
}

object Backend {
  import SQLParser._

  private val sqlParser = new SQLParser()

  def apply[PhysicalPlan: RenderTree, Config](planner: Planner[PhysicalPlan], evaluator: Evaluator[PhysicalPlan], ds: FileSystem, showNative: (PhysicalPlan, Path) => Cord) = new Backend {
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

    private def withString[A, B](name: String)(a: A, b: B)(render: (A, B) => Cord): EitherWriter[A] = {
      val result = PhaseResult.Detail(name, render(a, b).toString)

      EitherT[WriterResult, Error, A](
        WriterT.writer[Vector[PhaseResult], Error \/ A](
          (Vector.empty :+ result) -> \/- (a)))
    }

    def dataSource = ds

    // TODO: This should be extracted somewhere free of ServerConfig so it (and
    //       parts of it) can be used by tests.
    def plan(req: QueryRequest):
        (Vector[slamdata.engine.PhaseResult],
          slamdata.engine.Error \/ PhysicalPlan) = {
      import SemanticAnalysis._
      val phys = for {
        parsed     <- withTree("SQL AST")(sqlParser.parse(req.query))
        select     <- withTree("SQL AST (paths interpreted)")(interpretPaths(parsed, req.mountPath, req.basePath))
        tree       <- withTree("Annotated Tree")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
        logical    <- withTree("Logical Plan")(Compiler.compile(tree))
        simplified <- withTree("Simplified")(\/-(Optimizer.simplify(logical)))
        physical   <- withTree("Physical Plan")(planner.plan(simplified))
        _          <- withString("Mongo")(physical, req.out)(showNative)
      } yield physical
      phys.run.run
    }

    def run(req: QueryRequest): Task[(Vector[PhaseResult], Path)] = Task.delay {
      import Process.{logged => _, _}

      def loggedTask[A](log: Vector[PhaseResult], t: Task[A]): Task[(Vector[PhaseResult], A)] =
        new Task(t.get.map(_.bimap({
          case e : Error => PhaseError(log, e)
          case e => e
          },
          log -> _)))

      val (phases, physical) = plan(req)

      physical.fold[Task[(Vector[PhaseResult], Path)]](
        error => Task.fail(PhaseError(phases, error)),
        plan => loggedTask(phases, evaluator.execute(plan, req.out))
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
