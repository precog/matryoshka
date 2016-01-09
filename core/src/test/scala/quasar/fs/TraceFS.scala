package quasar.fs

import quasar.Predef._

import quasar.fp.free.{Interpreter}

import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

/** Interpreter that just records a log of the actions that are performed. */
object TraceFS {
  val FsType = FileSystemType("trace")

  type __FSAction0[A] = Coproduct[WriteFile, ReadFile, A]
  type __FSAction1[A] = Coproduct[ManageFile, __FSAction0, A]
  type FSAction[A] = Coproduct[QueryFile, __FSAction1, A]

  type Trace[A] = Writer[Vector[FSAction[_]], A]

  def qfTrace(nodes: Map[ADir, Set[Node]]) = new (QueryFile ~> Writer[Vector[QueryFile[_]], ?]) {
    import QueryFile._

    def ls(dir: ADir) = nodes.getOrElse(dir, Set())

    def apply[A](qf: QueryFile[A]): Writer[Vector[QueryFile[_]], A] =
      WriterT.writer((Vector(qf),
        qf match {
          case ExecutePlan(lp, out) => (Vector.empty, \/-(out))
          case EvaluatePlan(lp)     => (Vector.empty, \/-(ResultHandle(0)))
          case More(handle)         => \/-(Vector.empty)
          case Close(handle)        => ()
          case Explain(lp)          => (Vector.empty, \/-(ExecutionPlan(FsType, lp.toString)))
          case ListContents(dir)    => \/-(ls(dir))
          case FileExists(file)     =>
            val relevantNodes = ls(fileParent(file))
            val rFile = file1[Sandboxed](fileName(file))
            (relevantNodes.exists(node => node === Node.Case.Plain(rFile) || node === Node.Case.View(rFile))).right
        }))
  }

  def rfTrace = new (ReadFile ~> Writer[Vector[ReadFile[_]], ?]) {
    import ReadFile._

    def apply[A](rf: ReadFile[A]): Writer[Vector[ReadFile[_]], A] =
      WriterT.writer((Vector(rf),
        rf match {
          case Open(file, off, lim) => \/-(ReadHandle(file, 0))
          case Read(handle)         => \/-(Vector.empty)
          case Close(handle)        => ()
        }))
  }

  def wfTrace = new (WriteFile ~> Writer[Vector[WriteFile[_]], ?]) {
    import WriteFile._

    def apply[A](wf: WriteFile[A]): Writer[Vector[WriteFile[_]], A] =
      WriterT.writer((Vector(wf),
        wf match {
          case Open(file)           => \/-(WriteHandle(file, 0))
          case Write(handle, chunk) => Vector.empty
          case Close(handle)        => ()
        }))
  }

  def mfTrace = new (ManageFile ~> Writer[Vector[ManageFile[_]], ?]) {
    import ManageFile._

    def apply[A](mf: ManageFile[A]): Writer[Vector[ManageFile[_]], A] =
      WriterT.writer((Vector(mf),
        mf match {
          case Move(scenario, semantics) => \/-(())
          case Delete(path)              => \/-(())
          case TempFile(maybeNear) =>
            maybeNear.fold[ADir](rootDir)(fileParent(_)) </> file("tmp")
        }))
  }

  val DefaultNodes = Map[ADir, Set[Node]](rootDir -> Set(
    Node.Plain(currentDir </> file("afile")),
    Node.Plain(currentDir </> dir("adir"))))

  def traceFs: FileSystem ~> Trace =
    interpretFileSystem[Writer[Vector[FSAction[_]], ?]](
      inj[QueryFile] compose qfTrace(DefaultNodes),
      inj[ReadFile] compose rfTrace,
      inj[WriteFile] compose wfTrace,
      inj[ManageFile] compose mfTrace)

  def inj[F[_]](implicit I: F :<: FSAction) =
    new (Writer[Vector[F[_]], ?] ~> Writer[Vector[FSAction[_]], ?]) {
      def apply[A](w: Writer[Vector[F[_]], A]): Writer[Vector[FSAction[_]], A] =
        w.mapWritten(_.map(I.inj(_)))
      }

  def traceInterp[A](t: Free[FileSystem, A]): (Vector[FSAction[_]], A) = {
    new Interpreter(traceFs).interpret(t).run
  }
}
