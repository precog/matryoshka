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

package quasar
package fs

import quasar.fp.free._
import quasar.recursionschemes._, FunctorT.ops._
import LogicalPlan.ReadF

import monocle.Optional
import monocle.function.Field1
import monocle.std.tuple2._
import pathy.{Path => PPath}, PPath._
import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.functor._

object chroot {

  /** Rebases all paths in `ReadFile` operations onto the given prefix. */
  def readFile(prefix: ADir): ReadFileF ~> ReadFileF = {
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

  def readFileIn[S[_]](prefix: ADir)(implicit S: ReadFileF :<: S): S ~> S =
    injectedNT[ReadFileF, S](readFile(prefix))

  /** Rebases all paths in `WriteFile` operations onto the given prefix. */
  def writeFile(prefix: ADir): WriteFileF ~> WriteFileF = {
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

  def writeFileIn[S[_]](prefix: ADir)(implicit S: WriteFileF :<: S): S ~> S =
    injectedNT[WriteFileF, S](writeFile(prefix))

  /** Rebases all paths in `ManageFile` operations onto the given prefix. */
  def manageFile(prefix: ADir): ManageFileF ~> ManageFileF = {
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
          Coyoneda.lift(Delete(rebase(p, prefix)))
            .map(_ leftMap stripPathError(prefix))

        case TempFile(nt) =>
          Coyoneda.lift(TempFile(nt map (rebase(_, prefix))))
            .map(stripPrefix(prefix))
      }
    }

    new (ManageFileF ~> ManageFileF) {
      def apply[A](mf: ManageFileF[A]) = mf flatMap f
    }
  }

  def manageFileIn[S[_]](prefix: ADir)(implicit S: ManageFileF :<: S): S ~> S =
    injectedNT[ManageFileF, S](manageFile(prefix))

  /** Rebases paths in `QueryFile` onto the given prefix. */
  def queryFile(prefix: ADir): QueryFileF ~> QueryFileF = {
    import QueryFile._

    val base = Path(posixCodec.printPath(prefix))

    val rebasePlan: LogicalPlan ~> LogicalPlan =
      new (LogicalPlan ~> LogicalPlan) {
        def apply[A](lp: LogicalPlan[A]) = lp match {
          case ReadF(p) => ReadF(base ++ p)
          case _        => lp
        }
      }

    val f = new (QueryFile ~> QueryFileF) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          Coyoneda.lift(ExecutePlan(lp.translate(rebasePlan), rebase(out, prefix)))
            .map(_.map(_.bimap(stripPathError(prefix), stripPrefix(prefix))))

        case EvaluatePlan(lp) =>
          Coyoneda.lift(EvaluatePlan(lp.translate(rebasePlan)))
            .map(_.map(_ leftMap stripPathError(prefix)))

        case More(h) =>
          Coyoneda.lift(More(h))
            .map(_ leftMap stripPathError(prefix))

        case Close(h) =>
          Coyoneda.lift(Close(h))

        case Explain(lp) =>
          Coyoneda.lift(Explain(lp.translate(rebasePlan)))
            .map(_.map(_ leftMap stripPathError(prefix)))

        case ListContents(d) =>
          Coyoneda.lift(ListContents(rebase(d, prefix)))
            .map(_.bimap(stripPathError(prefix), _ map stripNodePrefix(prefix)))

        case FileExists(f) => Coyoneda.lift(FileExists(rebase(f,prefix)))
            .map(_.leftMap(stripPathError(prefix)))
      }
    }

    new (QueryFileF ~> QueryFileF) {
      def apply[A](qf: QueryFileF[A]) = qf flatMap f
    }
  }

  def queryFileIn[S[_]](prefix: ADir)(implicit S: QueryFileF :<: S): S ~> S =
    injectedNT[QueryFileF, S](queryFile(prefix))

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def fileSystem[S[_]](prefix: ADir)
                      (implicit S0: ReadFileF :<: S,
                                S1: WriteFileF :<: S,
                                S2: ManageFileF :<: S,
                                S3: QueryFileF :<: S)
                      : S ~> S = {

    readFileIn[S](prefix)   compose
    writeFileIn[S](prefix)  compose
    manageFileIn[S](prefix) compose
    queryFileIn[S](prefix)
  }

  ////

  private val fsPathError: Optional[FileSystemError, APath] =
    FileSystemError.pathError composeLens PathError2.errorPath

  private val fsPlannerError: Optional[FileSystemError, Fix[LogicalPlan]] =
    FileSystemError.plannerError composeLens Field1.first

  // TODO: AbsDir relativeTo rootDir doesn't need to be partial, add the appropriate method to pathy
  private def rebase[T](p: PPath[Abs,T,Sandboxed], onto: ADir): PPath[Abs,T,Sandboxed] =
    p.relativeTo(rootDir[Sandboxed]).fold(p)(onto </> _)

  private def stripPathError(prefix: ADir): FileSystemError => FileSystemError = {
    val base = Path(posixCodec.printPath(prefix))

    val stripRead: LogicalPlan ~> LogicalPlan =
      new (LogicalPlan ~> LogicalPlan) {
        def apply[A](lp: LogicalPlan[A]) = lp match {
          case ReadF(p) => ReadF(p.rebase(base).map(_.asAbsolute) | p)
          case _        => lp
        }
      }

    val stripPlan: Fix[LogicalPlan] => Fix[LogicalPlan] =
      _ translate stripRead

    fsPathError.modify(stripAPathPrefix(prefix)) compose
      fsPlannerError.modify(stripPlan)
  }

  private def stripNodePrefix(prefix: ADir): Node => Node =
    _.fold(
      Node.Mount compose stripRelPrefix(prefix),
      Node.Plain compose stripRPathPrefix(prefix),
      Node.View  compose stripRelPrefix(prefix))

  private def stripAPathPrefix(prefix: ADir): APath => APath =
    p => stripPrefix(prefix)(p)

  private def stripRPathPrefix(prefix: ADir): RPath => RPath =
    p => stripRelPrefix(prefix)(p)

  private def stripRelPrefix[T](prefix: ADir): PPath[Rel, T, Sandboxed] => PPath[Rel, T, Sandboxed] =
    p => prefix.relativeTo(rootDir).flatMap(p relativeTo _) getOrElse p

  private def stripPrefix[T](prefix: ADir): PPath[Abs, T, Sandboxed] => PPath[Abs, T, Sandboxed] =
    p => p.relativeTo(prefix).fold(p)(rootDir </> _)
}
