package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.fs._

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

  import slamdata.engine.{Error => SDError}

  final case class Error(name: String, value: SDError) extends PhaseResult {
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

sealed trait Backend { self =>
  import Backend._

  def checkCompatibility: Task[slamdata.engine.Error \/ Unit]

  /**
   * Executes a query, producing a compilation log and the path where the result
   * can be found.
   */
  def run(req: QueryRequest): (Vector[PhaseResult], PathTask[ResultPath]) = {
    val (phases, pathT) = run0(QueryRequest(
      slamdata.engine.sql.SQLParser.mapPathsM[Id](req.query, _.asRelative),
      req.out.map(_.asRelative),
      req.variables))
    phases -> pathT.map {
      case ResultPath.User(path) => ResultPath.User(path.asAbsolute)
      case ResultPath.Temp(path) => ResultPath.Temp(path.asAbsolute)
    }
  }

  def run0(req: QueryRequest): (Vector[PhaseResult], PathTask[ResultPath])

  /**
    * Executes a query, placing the output in the specified resource, returning
    * both a compilation log and a source of values from the result set.
    */
  def eval(req: QueryRequest): (Vector[PhaseResult], Process[PathTask, Data]) = {
    val (log, outT) = run(req)
    (log,
      for {
        _       <- Process.eval(req.out.fold(().point[PathTask])(delete))
        out     <- Process.eval(outT)
        results = scanAll(out.path)
        rez     <- out match {
          case ResultPath.Temp(path) => results.cleanUpWith(delete(path).map(ignore))
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
  def evalResults(req: QueryRequest): Process[PathTask, Data] = eval(req)._2

  // Filesystem stuff

  final def scan(path: Path, offset: Option[Long], limit: Option[Long]):
      Process[PathTask, Data] =
    (offset, limit) match {
      // NB: skip < 0 is an error in the driver
      case (Some(o), _) if o < 0 => Process.fail(InvalidOffsetError(o))
      // NB: limit == 0 means no limit, and limit < 0 means request only a single batch (which we use below)
      case (_, Some(l)) if l < 1 => Process.fail(InvalidLimitError(l))
      case _ => scan0(path.asRelative, offset, limit)
    }

  def scan0(path: Path, offset: Option[Long], limit: Option[Long]):
      Process[PathTask, Data]

  final def scanAll(path: Path) = scan(path, None, None)

  final def scanTo(path: Path, limit: Long) = scan(path, None, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, Some(offset), None)

  def count(path: Path): PathTask[Long] = count0(path.asRelative)

  def count0(path: Path): PathTask[Long]

  /**
    Save a collection of documents at the given path, replacing any previous
    contents, atomically. If any error occurs while consuming input values,
    nothing is written and any previous values are unaffected.
    */
  def save(path: Path, values: Process[Task, Data]): PathTask[Unit] =
    save0(path.asRelative, values)

  def save0(path: Path, values: Process[Task, Data]): PathTask[Unit]

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
      Process[PathTask, WriteError] =
    append0(path.asRelative, values)

  def append0(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError]

  def move(src: Path, dst: Path): PathTask[Unit] =
    move0(src.asRelative, dst.asRelative)

  def move0(src: Path, dst: Path): PathTask[Unit]

  def delete(path: Path): PathTask[Unit] =
    delete0(path.asRelative)

  def delete0(path: Path): PathTask[Unit]

  def ls(dir: Path): PathTask[Set[FilesystemNode]] =
    dir.file.fold(
      ls0(dir.asRelative))(
      κ(EitherT.left(Task.now(InvalidPathError("Can not ls a file: " + dir)))))

  def ls0(dir: Path): PathTask[Set[FilesystemNode]]

  def ls: PathTask[Set[FilesystemNode]] = ls(Path.Root)

  def exists(path: Path): PathTask[Boolean] =
    if (path == Path.Root) true.point[PathTask]
    else ls(path.parent).map(_.map(path.parent ++ _.path) contains path)

  def defaultPath: Path
}

/** May be mixed in to implement the query methods of Backend using a Planner and Evaluator. */
trait PlannerBackend[PhysicalPlan] extends Backend {
  import Backend._

  def planner: Planner[PhysicalPlan]
  def evaluator: Evaluator[PhysicalPlan]
  implicit def RP: RenderTree[PhysicalPlan]

  lazy val queryPlanner = planner.queryPlanner(evaluator.compile(_))

  def checkCompatibility = evaluator.checkCompatibility

  def run0(req: QueryRequest): (Vector[PhaseResult], PathTask[ResultPath]) = {
    val (phases, physical) = queryPlanner(req)

    phases ->
      physical.fold[PathTask[ResultPath]](
        error => liftP(Task.fail(PhaseError(phases, error))),
        plan => {
          val rez1 = liftP(evaluator.execute(plan))
          for {
            rez0    <- rez1
            renamed <- (rez0, req.out) match {
              case (ResultPath.Temp(path), Some(out)) => for {
                _ <- move(path, out)
              } yield ResultPath.User(out)
              case _ => rez1
            }
          } yield renamed
        })
  }
}

object Backend {
  trait ScanError extends slamdata.engine.Error
  final case class InvalidOffsetError(value: Long) extends ScanError {
    def message = "invalid offset: " + value + " (must be >= 0)"
  }
  final case class InvalidLimitError(value: Long) extends ScanError {
    def message = "invalid limit: " + value + " (must be >= 1)"
  }

  type PathTask[X] = EitherT[Task, PathError, X]

  implicit def PathTaskCatchable = new Catchable[PathTask] {
    def attempt[A](f: PathTask[A]) =
      EitherT(f.run.attempt.map(_.fold(
        e => \/-(-\/(e)),
        _.fold(-\/(_), x => \/-(\/-(x))))))

    def fail[A](err: Throwable) = EitherT.right(Task.fail(err))
  }

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
    final case class Failure(error: Throwable, log: Cord) extends TestResult
  }
  def test(config: BackendConfig): Task[TestResult] = {
    val tests = for {
      backend <- BackendDefinitions.All(config).fold[PathTask[Backend]](
        Catchable[PathTask].fail(new RuntimeException("no backend for config: " + config)))(
        liftP)
      _ <- liftP(backend.checkCompatibility).flatMap[Unit](_.fold(Catchable[PathTask].fail, _.point[PathTask]))
      paths <- backend.ls

      // TODO:
      // tmp = generate temp Path
      // fs.exists(tmp)  // should be false
      // fs.save(tmp, valuesProcess)
      // fs.scan(tmp, None, None)
      // fs.delete(tmp)

    } yield Cord.mkCord(Cord("\n"), (Cord("Found files:") :: paths.toList.map(p => Cord(p.toString))): _*)

    tests.run.attempt.map(_.fold(
      TestResult.Failure(_, ""),
      _.fold(TestResult.Failure(_, ""), TestResult.Success)))
  }
}

/**
  Multi-mount backend that delegates each request to a single mount.
  Any request that references paths in more than one mount will fail.
*/
final case class NestedBackend(sourceMounts: Map[DirNode, Backend]) extends Backend {
  import Backend._

  // We use a var because we can’t leave the user with a copy that has a
  // reference to a concrete backend that no longer exists.
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Var"))
  private var mounts = sourceMounts

  private def nodePath(node: DirNode) = Path(List(DirNode.Current, node), None)

  def checkCompatibility: Task[slamdata.engine.Error \/ Unit] =
    mounts.values.toList.map(_.checkCompatibility).sequenceU.map(_.sequenceU.map(κ(())))

  def run0(req: QueryRequest): (Vector[PhaseResult], PathTask[ResultPath]) = {
    mounts.map { case (mountDir, backend) =>
      val mountPath = nodePath(mountDir)
      (for {
        q <- slamdata.engine.sql.SQLParser.mapPathsE(req.query, _.rebase(mountPath))
        out <- req.out.map(_.rebase(mountPath).map(Some(_))).getOrElse(\/-(None))
      } yield (backend, mountPath, QueryRequest(q, out, req.variables))).toOption
    }.toList.flatten match {
      case (backend, mountPath, req) :: Nil =>
        val (phases, t) = backend.run0(req)
        phases -> t.map {
          case ResultPath.User(path) => ResultPath.User(mountPath ++ path)
          case ResultPath.Temp(path) => ResultPath.Temp(mountPath ++ path)
        }
      case Nil =>
        // TODO: Restore this error message when #771 is fixed.
        // val err = InternalPathError("no single backend can handle all paths for request: " + req)
        val err = InvalidPathError("the request either contained a nonexistent path or could not be handled by a single backend: " + req.query)
        (Vector(PhaseResult.Error("Paths", err)), EitherT.left(Task.now(err)))
      case _   =>
        val err = InternalPathError("multiple backends can handle all paths for request: " + req)
        (Vector(PhaseResult.Error("Paths", err)), EitherT.left(Task.now(err)))
    }
  }

  def scan0(path: Path, offset: Option[Long], limit: Option[Long]):
      Process[PathTask, Data] =
    delegateP(path)(_.scan0(_, offset, limit))

  def count0(path: Path): PathTask[Long] = delegate(path)(_.count0(_))

  def save0(path: Path, values: Process[Task, Data]): PathTask[Unit] =
    delegate(path)(_.save0(_, values))

  def append0(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError] =
    delegateP(path)(_.append0(_, values))

  def move0(src: Path, dst: Path): PathTask[Unit] =
    delegate(src)((srcBackend, srcPath) => delegate(dst)((dstBackend, dstPath) =>
      if (srcBackend == dstBackend)
        srcBackend.move0(srcPath, dstPath)
      else
        EitherT.left(Task.now(InternalPathError("src and dst path not in the same backend")))))

  def delete0(path: Path): PathTask[Unit] = path.dir match {
    case Nil =>
      path.file.fold(
        // delete all children because a parent (and us) was deleted
        mounts.toList.map(_._2.delete0(path)).sequenceU.map(_.concatenate))(
        κ(EitherT.left(Task.now(NonexistentPathError(path, None)))))
    case last :: Nil => for {
      _ <- delegate(path)(_.delete0(_))
      _ <- EitherT.right(path.file.fold(
        Task.delay{ mounts = mounts - last })(
        κ(().point[Task])))
    } yield ()
    case _ => delegate(path)(_.delete0(_))
  }

  def ls0(dir: Path): PathTask[Set[FilesystemNode]] =
    if (dir == Path.Current)
      EitherT.right(Task.now(mounts.toSet[(DirNode, Backend)].map { case (d, b) =>
        FilesystemNode(nodePath(d), b match {
          case NestedBackend(_) => Plain
          case _                => Mount
        })
      }))
      else delegate(dir)(_.ls0(_))

  def defaultPath = Path.Current

  private def delegate[A](path: Path)(f: (Backend, Path) => PathTask[A]): PathTask[A] =
    path.asAbsolute.dir.headOption.fold[PathTask[A]](
      EitherT.left(Task.now(NonexistentPathError(path, None))))(
      node => mounts.get(node).fold(
        EitherT.left[Task, PathError, A](Task.now(NonexistentPathError(path, None))))(
        b => EitherT(Task.now(path.rebase(nodePath(node)))).flatMap(f(b, _))))

  private def delegateP[A](path: Path)(f: (Backend, Path) => Process[PathTask, A]): Process[PathTask, A] =
    path.asAbsolute.dir.headOption.fold[Process[PathTask, A]](
      Process.eval[PathTask, A](EitherT.left(Task.now(NonexistentPathError(path, None)))))(
      node => mounts.get(node).fold(
        Process.eval[PathTask, A](EitherT.left(Task.now(NonexistentPathError(path, None)))))(
        b => Process.eval[PathTask, Path](EitherT(Task.now(path.rebase(nodePath(node))))).flatMap(f(b, _))))
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
