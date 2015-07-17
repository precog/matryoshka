package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.Errors._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._
import scalaz.concurrent.{Node => _, _}

import scalaz.stream.{Writer => _, _}

import slamdata.engine.config._


sealed trait PhaseResult {
  def name: String
}
object PhaseResult {
  import argonaut._
  import Argonaut._

  final case class Error(name: String, value: CompilationError) extends PhaseResult {
    override def toString = name + "\n" + value.toString
  }
  final case class Tree(name: String, value: RenderedTree) extends PhaseResult {
    override def toString = name + "\n" + Show[RenderedTree].shows(value)
  }
  final case class Detail(name: String, value: String) extends PhaseResult {
    override def toString = name + "\n" + value
  }

  implicit def PhaseResultEncodeJson: EncodeJson[PhaseResult] = EncodeJson {
    case PhaseResult.Error(name, value) =>
      Json.obj(
        "name"  := name,
        "error" := value.message
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

sealed trait Backend { self =>
  import Backend._

  def checkCompatibility: ETask[EnvironmentError, Unit]

  /**
   * Executes a query, producing a compilation log and the path where the result
   * can be found.
   */
  def run(req: QueryRequest):
      (Vector[PhaseResult], ETask[EvaluationError, ResultPath])

  /**
    * Executes a query, placing the output in the specified resource, returning
    * both a compilation log and a source of values from the result set.
    */
  def eval(req: QueryRequest):
      (Vector[PhaseResult], Process[ETask[ProcessingError, ?], Data]) = {
    val (log, outT) = run(req)
    (log,
      for {
        out     <- Process.eval[ETask[ProcessingError, ?], ResultPath](outT.leftMap[ProcessingError](PEvalError))
        results = scanAll(out.path).translate[ETask[ProcessingError, ?]](convertError(PResultError))
        rez     <- out match {
          case ResultPath.Temp(path) => results.cleanUpWith(delete(path).leftMap[ProcessingError](PPathError))
          case _                     => results
        }
      } yield rez)
  }

  /**
    Executes a query, placing the output in the specified resource, returning
    only a compilation log.
    */
  def evalLog(req: QueryRequest): Vector[PhaseResult] = eval(req)._1

  /**
    Executes a query, placing the output in the specified resource, returning
    only a source of values from the result set.
    */
  def evalResults(req: QueryRequest): Process[ETask[ProcessingError, ?], Data] =
    eval(req)._2

  // Filesystem stuff

  final def scan(path: Path, offset: Long, limit: Option[Long]):
      Process[ETask[ResultError, ?], Data] =
    (offset, limit) match {
      // NB: skip < 0 is an error in the driver
      case (o, _)       if o < 0 => Process.eval[ETask[ResultError, ?], Data](EitherT.left(Task.now(InvalidOffsetError(o))))
      // NB: limit == 0 means no limit, and limit < 0 means request only a single batch (which we use below)
      case (_, Some(l)) if l < 1 => Process.eval[ETask[ResultError, ?], Data](EitherT.left(Task.now(InvalidLimitError(l))))
      case _                     => scan0(path, offset, limit)
    }

  def scan0(path: Path, offset: Long, limit: Option[Long]):
      Process[ETask[ResultError, ?], Data]

  final def scanAll(path: Path) = scan(path, 0, None)

  final def scanTo(path: Path, limit: Long) = scan(path, 0, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, offset, None)

  def count(path: Path): PathTask[Long]

  /**
    Save a collection of documents at the given path, replacing any previous
    contents, atomically. If any error occurs while consuming input values,
    nothing is written and any previous values are unaffected.
    */
  def save(path: Path, values: Process[Task, Data]): PathTask[Unit]

  /**
    Create a new collection of documents at the given path.
    */
  def create(path: Path, values: Process[Task, Data]) =
    // TODO: Fix race condition (#778)
    exists(path).flatMap(ex =>
      if (ex) EitherT.left[Task, PathError, Unit](Task.now(ExistingPathError(path, Some("can’t be created, because it already exists"))))
      else save(path, values))

  /**
    Replaces a collection of documents at the given path. If any error occurs,
    the previous contents should be unaffected.
  */
  def replace(path: Path, values: Process[Task, Data]) =
    // TODO: Fix race condition (#778)
    exists(path).flatMap(ex =>
      if (ex) save(path, values)
      else EitherT.left[Task, PathError, Unit](Task.now(NonexistentPathError(path, Some("can’t be replaced, because it doesn’t exist")))))

  /**
   Add values to a possibly existing collection. May write some values and not others,
   due to bad input or problems on the backend side. The result stream yields an error
   for each input value that is not written, or no values at all.
   */
  def append(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError]

  def move(src: Path, dst: Path): PathTask[Unit]

  def delete(path: Path): PathTask[Unit]

  def ls(dir: Path): PathTask[Set[FilesystemNode]]

  def ls: PathTask[Set[FilesystemNode]] = ls(Path.Root)

  def exists(path: Path): PathTask[Boolean] =
    if (path == Path.Root) true.point[PathTask]
    else ls(path.parent).map(_.map(path.parent ++ _.path) contains path)

  def defaultPath: Path
}

/** May be mixed in to implement the query methods of Backend using a Planner and Evaluator. */
trait PlannerBackend[PhysicalPlan] extends Backend {
  def planner: Planner[PhysicalPlan]
  def evaluator: Evaluator[PhysicalPlan]
  implicit def RP: RenderTree[PhysicalPlan]

  lazy val queryPlanner = planner.queryPlanner(evaluator.compile(_))

  def checkCompatibility = evaluator.checkCompatibility

  def run(req: QueryRequest): (Vector[PhaseResult], ETask[EvaluationError, ResultPath]) = {
    val (phases, physical) = queryPlanner(req)

    phases ->
      physical.fold[ETask[EvaluationError, ResultPath]](
        error => EitherT.left(Task.now(CompileFailed)),
        plan => {
          val rez1 = evaluator.execute(plan)
          for {
            rez0    <- rez1
            renamed <- (rez0, req.out) match {
              case (ResultPath.Temp(path), Some(out)) => for {
                _ <- move(path, out).leftMap[EvaluationError](EvalPathError)
              } yield ResultPath.User(out)
              case _ => rez1
            }
          } yield renamed
        })
  }
}

object Backend {
  trait ScanError extends slamdata.engine.ResultError
  final case class InvalidOffsetError(value: Long) extends ScanError {
    def message = "invalid offset: " + value + " (must be >= 0)"
  }
  final case class InvalidLimitError(value: Long) extends ScanError {
    def message = "invalid limit: " + value + " (must be >= 1)"
  }

  type PathTask[X] = ETask[PathError, X]
  val liftP = new (Task ~> PathTask) {
    def apply[T](t: Task[T]): PathTask[T] = EitherT.right(t)
  }

  implicit class PrOpsTask[O](self: Process[PathTask, O])
      extends PrOps[PathTask, O](self)

  trait PathNodeType
  final case object Mount extends PathNodeType
  final case object Plain extends PathNodeType

  final case class FilesystemNode(path: Path, typ: PathNodeType)

  implicit val FilesystemNodeOrder: scala.Ordering[FilesystemNode] =
    scala.Ordering[Path].on(_.path)

  sealed trait TestResult {
    def log: Cord
  }
  object TestResult {
    final case class Success(log: Cord) extends TestResult
    final case class Failure(error: EnvironmentError, log: Cord)
        extends TestResult
    final case class Error(error: Throwable, log: Cord) extends TestResult
  }
  def test(config: BackendConfig): Task[TestResult] = {
    val tests = for {
      backend <- BackendDefinitions.All(config).fold[ETask[EnvironmentError, Backend]](
        EitherT.left(Task.now(MissingBackend("no backend in config: " + config))))(
        EitherT.right(_))
      _     <- backend.checkCompatibility
      paths <- backend.ls.leftMap[EnvironmentError](EnvPathError)

      // TODO:
      // tmp = generate temp Path
      // fs.exists(tmp)  // should be false
      // fs.save(tmp, valuesProcess)
      // fs.scan(tmp, None, None)
      // fs.delete(tmp)

    } yield Cord.mkCord(Cord("\n"), (Cord("Found files:") :: paths.toList.map(p => Cord(p.toString))): _*)

    tests.run.attempt.map(_.fold(
      TestResult.Error(_, ""),
      _.fold(TestResult.Failure(_, ""), TestResult.Success)))
  }
}

/**
  Multi-mount backend that delegates each request to a single mount.
  Any request that references paths in more than one mount will fail.
*/
final case class NestedBackend(mounts: Map[Path, Backend]) extends Backend {
  import Backend._

  def checkCompatibility: ETask[EnvironmentError, Unit] =
    mounts.values.toList.map(_.checkCompatibility).sequenceU.map(κ(()))

  def run(req: QueryRequest): (Vector[PhaseResult], ETask[EvaluationError, ResultPath]) = {
    mounts.toList.map[Option[(Backend, Path, QueryRequest)], List[Option[(Backend, Path, QueryRequest)]]] { case (mountPath, backend) =>
      val mountDir = mountPath.asDir
      (for {
        q <- slamdata.engine.sql.SQLParser.mapPathsE(req.query, _.rebase(mountDir))
        out <- req.out.map(_.rebase(mountDir).map(Some(_))).getOrElse(\/-(None))
      } yield QueryRequest(q, out, req.variables)).toOption.map((backend, mountDir, _))
    }.flatten match {
      case (backend: Backend, mountDir: Path, req: QueryRequest) :: Nil =>
        val (phases, t) = backend.run(req)
        phases -> t.map {
          case ResultPath.User(path) => ResultPath.User(mountDir ++ path)
          case ResultPath.Temp(path) => ResultPath.Temp(mountDir ++ path)
        }
      case Nil =>
        // TODO: Restore this error message when #771 is fixed.
        // val err = InternalPathError("no single backend can handle all paths for request: " + req)
                           val err = InvalidPathError("the request either contained a nonexistent path or could not be handled by a single backend")
                           (Vector(PhaseResult.Error("Paths", CompilePathError(err))), EitherT.left(Task.now(CompileFailed)))
                           case _   =>
                           val err = InternalPathError("multiple backends can handle all paths for request: " + req)
                           (Vector(PhaseResult.Error("Paths", CompilePathError(err))), EitherT.left(Task.now(CompileFailed)))
                           }
  }

  def scan0(path: Path, offset: Long, limit: Option[Long]):
      Process[ETask[ResultError, ?], Data] =
    delegateP(path)(_.scan0(_, offset, limit), ResultPathError)

  def count(path: Path): PathTask[Long] = delegate(path)(_.count(_))

  def save(path: Path, values: Process[Task, Data]): PathTask[Unit] =
    delegate(path)(_.save(_, values))

  def append(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError] =
    delegateP(path)(_.append(_, values), ɩ)

  def move(src: Path, dst: Path): PathTask[Unit] =
    delegate(src)((srcBackend, srcPath) => delegate(dst)((dstBackend, dstPath) =>
      if (srcBackend == dstBackend)
        srcBackend.move(srcPath, dstPath)
      else
        EitherT.left(Task.now(InternalPathError("src and dst path not in the same backend")))))

  def delete(path: Path): PathTask[Unit] =
    delegate(path)(_.delete(_))

  def ls(dir: Path): PathTask[Set[FilesystemNode]] = {
    val mnts = mounts.keys.map(_.asAbsolute).collect { case p if (p.parent == dir.asDir) => p }.map(p => FilesystemNode(p.asDir, Mount)).toSet
    relativize(dir).map { case (b, p) => b.ls(p) }.sequenceU.map(_.foldLeft(mnts.toSet)(_ ++ _))
  }

  def defaultPath = Path.Root

  private def delegate[A](path: Path)(f: (Backend, Path) => PathTask[A]): PathTask[A] =
    EitherT(Task.now(relativizeOne(path))).flatMap(f.tupled)

  private def delegateP[E, A](path: Path)(f: (Backend, Path) => Process[ETask[E, ?], A], ef: PathError => E): Process[ETask[E, ?], A] =
    relativizeOne(path).fold(
      e => Process.eval[ETask[E, ?], A](EitherT.left(Task.now(ef(e)))),
      f.tupled)

  private def relativizeOne[E, A](path: Path): PathError \/ (Backend, Path) =
    relativize(path) match {
      case (backend, relPath) :: Nil => \/-((backend, relPath))
      case Nil                       => -\/(NonexistentPathError(path, Some("no backend")))
      case _                         => -\/(InvalidPathError("multiple backends for path: " + path))
    }

  private def relativize(path: Path): List[(Backend, Path)] =
    if (path == Path.Root)
      mounts.get(Path.Root).fold[List[(Backend, Path)]](Nil)(b => List(b -> Path(".")))
    else
      mounts.map { case (p, b) => path.rebase(p.asDir).toOption.map(b -> _) }.toList.flatten
}

final case class BackendDefinition(create: PartialFunction[BackendConfig, Task[Backend]]) extends (BackendConfig => Option[Task[Backend]]) {
  def apply(config: BackendConfig): Option[Task[Backend]] = create.lift(config)
}

object BackendDefinition {
  implicit val BackendDefinitionMonoid = new Monoid[BackendDefinition] {
    def zero = BackendDefinition(PartialFunction.empty)

    def append(v1: BackendDefinition, v2: => BackendDefinition): BackendDefinition =
      BackendDefinition(v1.create.orElse(v2.create))
  }
}
