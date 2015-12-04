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

import quasar.LogicalPlan, LogicalPlan.ReadF
import quasar.fp.free.injectedNT
import quasar.recursionschemes._, FunctorT.ops._

import monocle.Optional
import monocle.function.Field1
import monocle.std.tuple2._
import pathy.{Path => PPath}, PPath._
import scalaz._
import scalaz.std.tuple._
import scalaz.syntax.functor._

object mounted {

  /** Strips the `mountPoint` off of all input paths in `ReadFile` operations
    * and restores it on output paths.
    */
   def readFile[S[_]: Functor](mountPoint: ADir)
                              (implicit S: ReadFileF :<: S)
                              : S ~> S = {
    import ReadFile._

    val g = new (ReadFile ~> ReadFileF) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Open(src, off, lim) =>
          Coyoneda.lift(Open(stripPrefixA(mountPoint)(src), off, lim))
            .map(_ leftMap rebasePathError(mountPoint))

        case Read(h) =>
          Coyoneda.lift(Read(h))
            .map(_ leftMap rebasePathError(mountPoint))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    injectedNT[ReadFileF, S](Coyoneda.liftTF(g))
  }

  /** Strips the `mountPoint` off of all input paths in `WriteFile` operations
    * and restores it on output paths.
    */
  def writeFile[S[_]: Functor](mountPoint: ADir)
                              (implicit S: WriteFileF :<: S)
                              : S ~> S = {
    import WriteFile._

    val g = new (WriteFile ~> WriteFileF) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(dst) =>
          Coyoneda.lift(Open(stripPrefixA(mountPoint)(dst)))
            .map(_ leftMap rebasePathError(mountPoint))

        case Write(h, d) =>
          Coyoneda.lift(Write(h, d))
            .map(_ map rebasePathError(mountPoint))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    injectedNT[WriteFileF, S](Coyoneda.liftTF(g))
  }

  /** Strips the `mountPoint` off of all input paths in `ManageFile` operations
    * and restores it on output paths.
    */
  def manageFile[S[_]: Functor](mountPoint: ADir)
                               (implicit S: ManageFileF :<: S)
                               : S ~> S = {
    import ManageFile._, MoveScenario._

    val g = new (ManageFile ~> ManageFileF) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scn, sem) =>
          Coyoneda.lift(Move(
            scn.fold(
              (src, dst) => DirToDir(
                stripPrefixA(mountPoint)(src),
                stripPrefixA(mountPoint)(dst)),
              (src, dst) => FileToFile(
                stripPrefixA(mountPoint)(src),
                stripPrefixA(mountPoint)(dst))),
            sem))
            .map(_ leftMap rebasePathError(mountPoint))

        case Delete(p) =>
          Coyoneda.lift(Delete(stripPrefixA(mountPoint)(p)))
            .map(_ leftMap rebasePathError(mountPoint))

        case TempFile(p) =>
          Coyoneda.lift(TempFile(stripPrefixA(mountPoint)(p)))
            .map(_ bimap (rebasePathError(mountPoint), rebaseA(_, mountPoint)))
      }
    }

    injectedNT[ManageFileF, S](Coyoneda.liftTF(g))
  }

  /** Strips the `mountPoint` off of all input paths in `QueryFile` operations
    * and restores it on output paths.
    */
  def queryFile[S[_]: Functor](mountPoint: ADir)
                              (implicit S: QueryFileF :<: S)
                              : S ~> S = {
    import QueryFile._

    val base = Path(posixCodec.printPath(mountPoint))

    val stripPlan: LogicalPlan ~> LogicalPlan =
      new (LogicalPlan ~> LogicalPlan) {
        def apply[A](lp: LogicalPlan[A]) = lp match {
          case ReadF(p) => ReadF(p.rebase(base).map(_.asAbsolute) | p)
          case _        => lp
        }
      }

    val g = new (QueryFile ~> QueryFileF) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          Coyoneda.lift(ExecutePlan(lp.translate(stripPlan), stripPrefixA(mountPoint)(out)))
            .map(_.map(_.bimap(rebasePathError(mountPoint), rebaseA(_, mountPoint))))

        case EvaluatePlan(lp) =>
          Coyoneda.lift(EvaluatePlan(lp.translate(stripPlan)))
            .map(_.map(_ leftMap rebasePathError(mountPoint)))

        case More(h) =>
          Coyoneda.lift(More(h))
            .map(_ leftMap rebasePathError(mountPoint))

        case Close(h) =>
          Coyoneda.lift(Close(h))

        case Explain(lp) =>
          Coyoneda.lift(Explain(lp.translate(stripPlan)))
            .map(_.map(_.leftMap(rebasePathError(mountPoint))))

        case ListContents(d) =>
          Coyoneda.lift(ListContents(stripPrefixA(mountPoint)(d)))
            .map(_.leftMap(rebasePathError(mountPoint)))

        case FileExists(f) =>
          Coyoneda.lift(FileExists(stripPrefixA(mountPoint)(f)))
            .map(_.leftMap(rebasePathError(mountPoint)))
      }
    }

    injectedNT[QueryFileF, S](Coyoneda.liftTF(g))
  }

  def fileSystem[S[_]: Functor](mountPoint: ADir)
                               (implicit S0: ReadFileF :<: S,
                                         S1: WriteFileF :<: S,
                                         S2: ManageFileF :<: S,
                                         S3: QueryFileF :<: S)
                               : S ~> S = {

    readFile[S](mountPoint)   compose
    writeFile[S](mountPoint)  compose
    manageFile[S](mountPoint) compose
    queryFile[S](mountPoint)
  }

  ////

  private val fsPathError: Optional[FileSystemError, APath] =
    FileSystemError.pathError composeLens PathError2.errorPath

  private val fsPlannerError: Optional[FileSystemError, Fix[LogicalPlan]] =
    FileSystemError.plannerError composeLens Field1.first

  private def rebasePathError(onto: ADir): FileSystemError => FileSystemError = {
    val base = Path(posixCodec.printPath(onto))

    val rebaseRead: LogicalPlan ~> LogicalPlan =
      new (LogicalPlan ~> LogicalPlan) {
        def apply[A](lp: LogicalPlan[A]) = lp match {
          case ReadF(p) => ReadF(base ++ p)
          case _        => lp
        }
      }

    val rebasePlan: Fix[LogicalPlan] => Fix[LogicalPlan] =
      _ translate rebaseRead

    fsPathError.modify(rebaseA(_, onto)) compose fsPlannerError.modify(rebasePlan)
  }
}
