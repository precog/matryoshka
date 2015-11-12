package quasar

import quasar.fp._
import quasar.fs.{Path => QPath, _}
import quasar.recursionschemes._

import monocle.Optional

import pathy._, Path._

import scalaz._

object chroot {

  /** Rebases all paths in `ReadFile` operations onto the given prefix. */
  def readFile(prefix: AbsDir[Sandboxed]): ReadFileF ~> ReadFileF = {
    import ReadFile._

    val f = new (ReadFile ~> ReadFileF) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Open(f, off, lim) =>
          Coyoneda.lift(Open(rebase(f, prefix), off, lim))
            .map(_ leftMap stripPathError(prefix))

        case Read(h) =>
          Coyoneda.lift(Read(h))
            .map(_ leftMap stripPathError(prefix))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    new (ReadFileF ~> ReadFileF) {
      def apply[A](rf: ReadFileF[A]) = rf flatMap f
    }
  }

  def readFileIn[S[_]](prefix: AbsDir[Sandboxed])(implicit S: ReadFileF :<: S): S ~> S =
    interpret.injectedNT[ReadFileF, S](readFile(prefix))

  /** Rebases all paths in `WriteFile` operations onto the given prefix. */
  def writeFile(prefix: AbsDir[Sandboxed]): WriteFileF ~> WriteFileF = {
    import WriteFile._

    val f = new (WriteFile ~> WriteFileF) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(f) =>
          Coyoneda.lift(Open(rebase(f, prefix)))
            .map(_ leftMap stripPathError(prefix))

        case Write(h, d) =>
          Coyoneda.lift(Write(h, d))
            .map(_ map stripPathError(prefix))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    new (WriteFileF ~> WriteFileF) {
      def apply[A](wf: WriteFileF[A]) = wf flatMap f
    }
  }

  def writeFileIn[S[_]](prefix: AbsDir[Sandboxed])(implicit S: WriteFileF :<: S): S ~> S =
    interpret.injectedNT[WriteFileF, S](writeFile(prefix))

  /** Rebases all paths in `ManageFile` operations onto the given prefix. */
  def manageFile(prefix: AbsDir[Sandboxed]): ManageFileF ~> ManageFileF = {
    import ManageFile._, MoveScenario._

    val f = new (ManageFile ~> ManageFileF) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scn, sem) =>
          Coyoneda.lift(Move(
            scn.fold(
              (src, dst) => DirToDir(rebase(src, prefix), rebase(dst, prefix)),
              (src, dst) => FileToFile(rebase(src, prefix), rebase(dst, prefix))),
            sem))
            .map(_ leftMap stripPathError(prefix))

        case Delete(p) =>
          Coyoneda.lift(Delete(p.bimap(rebase(_, prefix), rebase(_, prefix))))
            .map(_ leftMap stripPathError(prefix))

        case ListContents(d) =>
          Coyoneda.lift(ListContents(rebase(d, prefix)))
            .map(_.bimap(stripPathError(prefix), _ map stripNodePrefix(prefix)))

        case TempFile(nt) =>
          Coyoneda.lift(TempFile(nt map (rebase(_, prefix))))
            .map(stripPrefix(prefix))
      }
    }

    new (ManageFileF ~> ManageFileF) {
      def apply[A](mf: ManageFileF[A]) = mf flatMap f
    }
  }

  def manageFileIn[S[_]](prefix: AbsDir[Sandboxed])(implicit S: ManageFileF :<: S): S ~> S =
    interpret.injectedNT[ManageFileF, S](manageFile(prefix))

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def fileSystem[S[_]](prefix: AbsDir[Sandboxed])
                      (implicit S0: ReadFileF :<: S, S1: WriteFileF :<: S, S2: ManageFileF :<: S): S ~> S = {
    readFileIn[S](prefix) compose writeFileIn[S](prefix) compose manageFileIn[S](prefix)
  }

  /** Rebases paths in `ExecutePlan` onto the given prefix. */
  def executePlan(prefix: AbsDir[Sandboxed]): ExecutePlan ~> ExecutePlan = {
    import LogicalPlan.ReadF
    import ResultFile.resultFile
    import FunctorT.ops._

    val base = QPath(posixCodec.printPath(prefix))

    val rebasePlan: LogicalPlan ~> LogicalPlan =
      new (LogicalPlan ~> LogicalPlan) {
        def apply[A](lp: LogicalPlan[A]) = lp match {
          case ReadF(p) => ReadF(base ++ p)
          case _        => lp
        }
      }

    new (ExecutePlan ~> ExecutePlan) {
      def apply[A](ep: ExecutePlan[A]) =
        ExecutePlan(ep.lp.translate(rebasePlan), rebase(ep.out, prefix), {
          case (xs, r) =>
            ep.f((xs, r.map(resultFile.modify(stripPrefix(prefix)))))
        })
    }
  }

  def executePlanIn[S[_]](prefix: AbsDir[Sandboxed])(implicit S: ExecutePlan :<: S): S ~> S =
    interpret.injectedNT[ExecutePlan, S](executePlan(prefix))

  /** Rebases all paths in `QueryableFileSystem` operations onto the given prefix. */
  def queryableFileSystem[S[_]](prefix: AbsDir[Sandboxed])
                               (implicit S0: ReadFileF :<: S,
                                         S1: WriteFileF :<: S,
                                         S2: ManageFileF :<: S,
                                         S3: ExecutePlan :<: S)
                               : S ~> S = {

    executePlanIn[S](prefix) compose fileSystem[S](prefix)
  }

  ////

  import ManageFile.Node._

  private val fsPathError: Optional[FileSystemError, AbsPath[Sandboxed]] =
    FileSystemError.pathError composeLens PathError2.errorPath

  // TODO: AbsDir relativeTo rootDir doesn't need to be partial, add the appropriate method to pathy
  private def rebase[T](p: Path[Abs,T,Sandboxed], onto: AbsDir[Sandboxed]): Path[Abs,T,Sandboxed] =
    p.relativeTo(rootDir[Sandboxed]).fold(p)(onto </> _)

  private def stripPathError(prefix: AbsDir[Sandboxed]): FileSystemError => FileSystemError =
    fsPathError.modify(stripPathPrefix(prefix))

  private def stripPathPrefix(prefix: AbsDir[Sandboxed]): AbsPath[Sandboxed] => AbsPath[Sandboxed] =
    _.bimap(stripPrefix(prefix), stripPrefix(prefix))

  private def stripNodePrefix(prefix: AbsDir[Sandboxed]): ManageFile.Node => ManageFile.Node =
    _.fold(
      Mount compose stripRelPrefix(prefix),
      p => Plain(p.bimap(stripRelPrefix(prefix), stripRelPrefix(prefix))))

  private def stripRelPrefix[T](prefix: AbsDir[Sandboxed]): Path[Rel, T, Sandboxed] => Path[Rel, T, Sandboxed] =
    p => prefix.relativeTo(rootDir).flatMap(p relativeTo _) getOrElse p

  private def stripPrefix[T](prefix: AbsDir[Sandboxed]): Path[Abs, T, Sandboxed] => Path[Abs, T, Sandboxed] =
    p => p.relativeTo(prefix).fold(p)(rootDir </> _)
}
