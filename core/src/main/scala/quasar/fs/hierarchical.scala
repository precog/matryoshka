/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.fs

import quasar.Predef._
import quasar.{Data, LogicalPlan, PhaseResult, PhaseResults}
import quasar.effect._
import quasar.fp._
import quasar.recursionschemes.{free => _, _}, Recursive.ops._
import quasar.mount.Mounts

import monocle.{Iso, Prism}
import monocle.syntax.fields._
import monocle.std.tuple2._
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._

object hierarchical {
  import QueryFile.ResultHandle
  import ReadFile.ReadHandle
  import WriteFile.WriteHandle
  import FileSystemError._, PathError2._

  type HFSFailure[A]      = Failure[HierarchicalFileSystemError, A]
  type HFSFailureF[A]     = Coyoneda[HFSFailure, A]

  type MountedResultH[A]  = KeyValueStore[ResultHandle, (ADir, ResultHandle), A]
  type MountedResultHF[A] = Coyoneda[MountedResultH, A]

  type HFSErrT[F[_], A] = EitherT[F, HierarchicalFileSystemError, A]

  sealed trait HierarchicalFileSystemError

  object HierarchicalFileSystemError {
    final case class MultipleMountsApply private[fs] (mounts: Set[ADir], lp: Fix[LogicalPlan])
      extends HierarchicalFileSystemError

    val multipleMountsApply: Prism[HierarchicalFileSystemError, (Set[ADir], Fix[LogicalPlan])] =
      Prism[HierarchicalFileSystemError, (Set[ADir], Fix[LogicalPlan])] {
        case MultipleMountsApply(mnts, lp) => Some((mnts, lp))
        case _ => None
      } ((MultipleMountsApply(_, _)).tupled)

    implicit val hierarchicalFSErrorShow: Show[HierarchicalFileSystemError] =
      Show.shows {
        case MultipleMountsApply(mnts, lp) =>
          s"Multiple mounted filesystems can handle the plan: mounts=${mnts.shows}, plan=${lp.shows}"
      }
  }

  /** Returns a `ReadFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    *
    * @param mountSep used to separate the mount from the original file in handles
    * @param rfs `ReadFileF` interpreters indexed by mount
    */
  def readFile[F[_], S[_]](
    mountSep: DirName,
    rfs: Mounts[ReadFileF ~> F]
  )(implicit
    S0: Functor[S],
    S1: F :<: S
  ): ReadFileF ~> Free[S, ?] = {
    import ReadFile._

    type M[A] = Free[S, A]

    lazy val mountedRfs = rfs mapWithDir { case (d, f) =>
      f compose mounted.readFile[ReadFileF](d)
    }

    def evalRead[A](g: ReadFileF ~> F, ra: ReadFile[A]): M[A] =
      free.lift(g(Coyoneda.lift(ra))).into[S]

    val f = new (ReadFile ~> M) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Open(loc, off, lim) =>
          lookupMounted(mountedRfs, loc)
            .fold(pathError(pathNotFound(loc)).left[ReadHandle].point[M]) {
              case (mnt, g) =>
                evalRead(g, Open(loc, off, lim))
                  .map(_ map prefixH(mountSep, ReadHandle.tupleIso, mnt))
            }

        case Read(h) =>
          mntH(mountSep, ReadHandle.tupleIso, h).flatMap { case (mnt, origH) =>
            mountedRfs.lookup(mnt) map (evalRead(_, Read(origH)))
          } getOrElse unknownReadHandle(h).left.point[M]

        case Close(h) =>
          mntH(mountSep, ReadHandle.tupleIso, h).flatMap { case (mnt, origH) =>
            mountedRfs.lookup(mnt) map (evalRead(_, Close(origH)))
          } getOrElse ().point[M]
      }
    }

    Coyoneda.liftTF(f)
  }

  /** Returns a `WriteFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    *
    * @param mountSep used to separate the mount from the original file in handles
    * @param wfs `WriteFileF` interpreters indexed by mount
    */
  def writeFile[F[_], S[_]](
    mountSep: DirName,
    wfs: Mounts[WriteFileF ~> F]
  )(implicit
    S0: Functor[S],
    S1: F :<: S
  ): WriteFileF ~> Free[S, ?] = {
    import WriteFile._

    type M[A] = Free[S, A]

    lazy val mountedWfs = wfs mapWithDir { case (d, f) =>
      f compose mounted.writeFile[WriteFileF](d)
    }

    def evalWrite[A](g: WriteFileF ~> F, wa: WriteFile[A]): M[A] =
      free.lift(g(Coyoneda.lift(wa))).into[S]

    val f = new (WriteFile ~> M) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(loc) =>
          lookupMounted(mountedWfs, loc)
            .fold(pathError(pathNotFound(loc)).left[WriteHandle].point[M]) {
              case (mnt, g) =>
                evalWrite(g, Open(loc))
                  .map(_ map prefixH(mountSep, WriteHandle.tupleIso, mnt))
            }

        case Write(h, chunk) =>
          mntH(mountSep, WriteHandle.tupleIso, h).flatMap { case (mnt, origH) =>
            mountedWfs.lookup(mnt) map (evalWrite(_, Write(origH, chunk)))
          } getOrElse Vector(unknownWriteHandle(h)).point[M]

        case Close(h) =>
          mntH(mountSep, WriteHandle.tupleIso, h).flatMap { case (mnt, origH) =>
            mountedWfs.lookup(mnt) map (evalWrite(_, Close(origH)))
          } getOrElse ().point[M]
      }
    }

    Coyoneda.liftTF(f)
  }

  /** Returns a `ManageFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    */
  def manageFile[F[_], S[_]](
    mfs: Mounts[ManageFileF ~> F]
  )(implicit
    S0: Functor[S],
    S1: F :<: S
  ): ManageFileF ~> Free[S, ?] = {
    import ManageFile._

    type M[A] = Free[S, A]
    type ME[A, B] = EitherT[M, A, B]

    val mountedMfs = mfs mapWithDir { case (d, f) =>
      f compose mounted.manageFile[ManageFileF](d)
    }

    def evalManage[A](g: ManageFileF ~> F, ma: ManageFile[A]): M[A] =
      free.lift(g(Coyoneda.lift(ma))).into[S]

    val lookup = lookupMounted(mountedMfs, _: APath)

    def noMountError(path: APath) =
      pathError(invalidPath(path, "does not refer to a mounted filesystem"))

    val f = new (ManageFile ~> M) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scn, sem) =>
          val src = lookup(scn.src).toRightDisjunction(
            pathError(pathNotFound(scn.src)))

          val dst = lookup(scn.dst).toRightDisjunction(
            noMountError(scn.dst))

          EitherT.fromDisjunction[M](src tuple dst).flatMap {
            case ((srcMnt, g), (dstMnt, _)) if srcMnt == dstMnt =>
              EitherT(evalManage(g, Move(scn, sem)))

            case _ =>
              pathError(invalidPath(
                scn.dst,
                s"must refer to the same filesystem as '${posixCodec.printPath(scn.src)}'"
              )).raiseError[ME, Unit]
          }.run

        case Delete(path) =>
          refineType(path).fold(deleteDir, deleteFile)

        case TempFile(near) =>
          EitherT.fromDisjunction[M](
            lookup(near) toRightDisjunction noMountError(near)
          ).flatMapF { case (_, g) =>
            evalManage(g, TempFile(near))
          }.run
      }

      def deleteDir(d: ADir) =
        lookup(d) cata (
          { case (_, g) => evalManage(g, Delete(d)) },
          mountedMfs.toMap.filterKeys(_.relativeTo(d).isDefined)
            .toList
            .traverse { case (mnt, g) => evalManage(g, Delete(mnt)) }
            .map(_.traverseU_(x => x))) // NB: sequenceU_ doesn't exist =(

      def deleteFile(f: AFile) =
        EitherT.fromDisjunction[M](
          lookup(f) toRightDisjunction pathError(pathNotFound(f))
        ).flatMapF { case (_, g) =>
          evalManage(g, Delete(f))
        }.run
    }

    Coyoneda.liftTF(f)
  }

  /** Returns a `QueryFileF` interpreter that selects one of the configured
    * child interpreters based on the path of the incoming request.
    */
  def queryFile[F[_], S[_]](qfs: Mounts[QueryFileF ~> F])
                           (implicit S0: Functor[S],
                                     S1: F :<: S,
                                     S2: HFSFailureF :<: S,
                                     S3: MonotonicSeqF :<: S,
                                     S4: MountedResultHF :<: S)
                           : QueryFileF ~> Free[S, ?] = {
    import QueryFile._

    type M[A] = Free[S, A]

    val failure = Failure.Ops[HierarchicalFileSystemError, S]
    val seq     = MonotonicSeq.Ops[S]
    val handles = KeyValueStore.Ops[ResultHandle, (ADir, ResultHandle), S]
    val transforms = Transforms[M]
    import transforms._

    lazy val mountedQfs = qfs mapWithDir { case (d, f) =>
      f compose mounted.queryFile[QueryFileF](d)
    }

    def evalQuery[A](g: QueryFileF ~> F, qa: QueryFile[A]): M[A] =
      free.lift(g(Coyoneda.lift(qa))).into[S]

    val f = new (QueryFile ~> M) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          resultForPlan(lp, some(out), ExecutePlan(lp, out))
            .map(_._2).run.run

        case EvaluatePlan(lp) =>
          (for {
            r    <- resultForPlan(lp, none, EvaluatePlan(lp))
            newH <- toExec(seq.next.map(ResultHandle(_)))
            _    <- toExec(handles.put(newH, r))
          } yield newH).run.run

        case More(h) =>
          getMounted[S](h, mountedQfs)
            .toRight(unknownResultHandle(h))
            .flatMapF { case (qh, g) => evalQuery(g, More(qh)) }
            .run

        case Close(h) =>
          getMounted[S](h, mountedQfs)
            .flatMapF(handles.delete(h).as(_))
            .flatMapF { case (qh, g) => evalQuery(g, Close(qh)) }
            .getOrElse(())

        case Explain(lp) =>
          resultForPlan(lp, none, Explain(lp))
            .map(_._2).run.run

        case ListContents(d) =>
          lookupMounted(mountedQfs, d)
            .map { case (_, g) => evalQuery(g, ListContents(d)) }
            .orElse(
              lsMounts(mountedQfs.toMap.keySet, d)
                .map(_.right[FileSystemError].point[M]))
            .getOrElse(pathError(pathNotFound(d)).left.point[M])

        case FileExists(f) =>
          lookupMounted(mountedQfs, f)
            .map { case (_, g) => evalQuery(g, FileExists(f)) }
            .getOrElse(pathError(pathNotFound(f)).left.point[M])
      }

      def resultForPlan[A](
        lp: Fix[LogicalPlan],
        out: Option[AFile],
        qf: QueryFile[(PhaseResults, FileSystemError \/ A)]
      ): ExecM[(ADir, A)] =
        mountForPlan(mountedQfs, lp, out) match {
          case -\/(-\/(hfsErr)) =>
            toExec(failure.fail[(ADir, A)](hfsErr))

          case -\/(\/-(pErr)) =>
            EitherT.leftU[(ADir, A)](pathError(pErr).point[G])

          case \/-((mnt, g)) =>
            EitherT(WriterT(evalQuery(g, qf)): G[FileSystemError \/ A])
              .strengthL(mnt)
        }
    }

    Coyoneda.liftTF(f)
  }

  def fileSystem[F[_], S[_]](
    mountSep: DirName,
    mounts: Mounts[FileSystem ~> F]
  )(implicit
    S0: Functor[S],
    S1: F :<: S,
    S2: MountedResultHF :<: S,
    S3: MonotonicSeqF :<: S,
    S4: HFSFailureF :<: S
  ): FileSystem ~> Free[S, ?] = {
    type M[A] = Free[S, A]
    type FS[A] = FileSystem[A]

    def injFS[G[_]](implicit I: G :<: FS): G ~> FS = injectNT[G, FS]

    val qf: QueryFileF ~> M  = queryFile[F, S](mounts map (_ compose injFS[QueryFileF]))
    val rf: ReadFileF ~> M   = readFile[F, S](mountSep, mounts map (_ compose injFS[ReadFileF]))
    val wf: WriteFileF ~> M  = writeFile[F, S](mountSep, mounts map (_ compose injFS[WriteFileF]))
    val mf: ManageFileF ~> M = manageFile[F, S](mounts map (_ compose injFS[ManageFileF]))

    free.interpret4(qf, rf, wf, mf)
  }

  ////

  private def lookupMounted[A](mounts: Mounts[A], path: APath): Option[(ADir, A)] =
    mounts.toMap find { case (d, a) => path.relativeTo(d).isDefined }

  private def mountForPlan[A](
    mounts: Mounts[A],
    lp: Fix[LogicalPlan],
    out: Option[AFile]
  ): (HierarchicalFileSystemError \/ PathError2) \/ (ADir, A) = {
    import LogicalPlan._
    import HierarchicalFileSystemError._

    type MntA = (ADir, A)
    type F[A] = State[Option[MntA], A]
    type M[A] = EitherT[F, PathError2, A]

    val F = MonadState[State, Option[MntA]]

    def lookupMnt(p: APath): PathError2 \/ MntA =
      lookupMounted(mounts, p) toRightDisjunction PathNotFound(p)

    def compareToExisting(mnt: ADir): M[Unit] = {
      def errMsg(exMnt: ADir): PathError2 =
        InvalidPath(mnt, s"refers to a different filesystem than '${posixCodec.printPath(exMnt)}'")

      EitherT[F, PathError2, Unit](F.gets(exMnt =>
        exMnt map (_._1) filter (_ != mnt) map errMsg toLeftDisjunction (())
      ))
    }

    def mountFor(p: APath): M[Unit] = for {
      mntA     <- EitherT.fromDisjunction[F](lookupMnt(p))
      (mnt, a) =  mntA
      _        <- compareToExisting(mnt)
      _        <- F.put(some(mntA)).liftM[PathErr2T]
    } yield ()

    out.cata(d => lookupMnt(d) bimap (_.right, some), none.right) flatMap (initMnt =>
      lp.cataM[M, Unit] {
        case ReadF(p) => mountFor(p.asAPath)
        case _        => ().point[M]
      }.run.run(initMnt) match {
        // TODO: If mnt is empty, then there were no `ReadF`, so we should
        //       be able to get a result without needing an actual filesystem
        //       Though, if that is the case, the plan shouldn't even be handed
        //       to a filesystem in the first place.
        case (mntA, r) =>
          r.leftMap(_.right[HierarchicalFileSystemError]) *>
            mntA.toRightDisjunction(multipleMountsApply(mounts.toMap.keySet, lp).left)
      })
  }

  private def lsMounts(mounts: Set[ADir], ls: ADir): Option[Set[Node]] = {
    def mkNode(rdir: RDir): Option[Node] =
      flatten(none, none, none, n => dir(n).some, κ(none), rdir).toList.unite match {
        case d :: Nil => Node.Mount(d).some
        case d :: _   => Node.Plain(d).some
        case Nil      => none
      }

    mounts.toList.map(_ relativeTo ls flatMap mkNode).unite match {
      case Nil => none
      case xs  => xs.toSet.some
    }
  }

  /** Returns the extracted mount from the given handle and a new handle with
    * the mount prefix and separator removed.
    */
  private def mntH[H](sep: DirName, iso: Iso[H, (AFile, Long)], h: H): Option[(ADir, H)] = {
    import IList.{single, empty}
    type F[A] = Option[(ADir, A)]

    def splitAtSep(f: AFile): Option[(ADir, AFile)] = {
      val segs = {
        val ss = flatten[IList[String]](empty, empty, empty, single, single, f)
        (ss.head :: ss.tail).join
      }
      val mntSegs = segs.takeWhile(_ != sep.value)
      val fileSegs = segs.drop(mntSegs.length + 1)

      val mnt = mntSegs.foldLeft(rootDir[Sandboxed])(_ </> dir(_))
      val origF = fileSegs.toNel.map(_.reverse).map { nel =>
        nel.tail.foldRight(rootDir[Sandboxed])((s, d) => d </> dir(s)) </> file(nel.head)
      }

      origF strengthL mnt
    }

    iso.composeLens(_1)
      .modifyF[F](splitAtSep)(h)(Functor[Option].compose[(ADir, ?)])
  }

  /** Returns a function that adds a mount-specific prefix to a handle,
    * separated by `sep`.
    */
  private def prefixH[H](sep: DirName, iso: Iso[H, (AFile, Long)], mnt: ADir): H => H =
    iso.composeLens(_1) modify { f =>
      f.relativeTo(rootDir).fold(f)(rf => mnt </> dir1(sep) </> rf)
    }

  private object getMounted {
    final class Aux[S[_]] {
      type F[A] = Free[S, A]

      def apply[A, B](a: A, mnts: Mounts[B])
                     (implicit S: Functor[S], I: KeyValueStoreF[A, (ADir, A), ?] :<: S)
                     : OptionT[F, (A, B)] = {
        KeyValueStore.Ops[A, (ADir, A), S].get(a) flatMap { case (d, a1) =>
          OptionT(mnts.lookup(d).strengthL(a1).point[F])
        }
      }
    }

    def apply[S[_]]: Aux[S] = new Aux[S]
  }
}
