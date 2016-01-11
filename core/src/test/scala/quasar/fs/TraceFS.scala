package quasar.fs

import quasar.Predef._

import quasar._, RenderTree.ops._
import quasar.fp._
import quasar.fp.free.{Interpreter}

import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

/** Interpreter that just records a log of the actions that are performed. */
object TraceFS {
  val FsType = FileSystemType("trace")

  type Trace[A] = Writer[Vector[RenderedTree], A]

  def qfTrace(nodes: Map[ADir, Set[Node]]) = new (QueryFile ~> Trace) {
    import QueryFile._

    def ls(dir: ADir) = nodes.getOrElse(dir, Set())

    def apply[A](qf: QueryFile[A]): Trace[A] =
      WriterT.writer((Vector(qf.render),
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

  def rfTrace = new (ReadFile ~> Trace) {
    import ReadFile._

    def apply[A](rf: ReadFile[A]): Trace[A] =
      WriterT.writer((Vector(rf.render),
        rf match {
          case Open(file, off, lim) => \/-(ReadHandle(file, 0))
          case Read(handle)         => \/-(Vector.empty)
          case Close(handle)        => ()
        }))
  }

  def wfTrace = new (WriteFile ~> Trace) {
    import WriteFile._

    def apply[A](wf: WriteFile[A]): Trace[A] =
      WriterT.writer((Vector(wf.render),
        wf match {
          case Open(file)           => \/-(WriteHandle(file, 0))
          case Write(handle, chunk) => Vector.empty
          case Close(handle)        => ()
        }))
  }

  def mfTrace = new (ManageFile ~> Trace) {
    import ManageFile._

    def apply[A](mf: ManageFile[A]): Trace[A] =
      WriterT.writer((Vector(mf.render),
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
    interpretFileSystem[Trace](qfTrace(DefaultNodes), rfTrace, wfTrace, mfTrace)

  def traceInterp[A](t: Free[FileSystem, A]): (Vector[RenderedTree], A) = {
    new Interpreter(traceFs).interpret(t).run
  }
}
