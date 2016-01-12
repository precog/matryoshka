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
import quasar.recursionschemes.{FunctorT, Fix}, FunctorT.ops._

import monocle.Optional
import monocle.syntax.fields._
import monocle.std.tuple2._
import scalaz.{Optional => _, _}
import scalaz.std.tuple._
import scalaz.syntax.functor._

object transformPaths {

  /** Returns a natural transformation that transforms all paths in `ReadFile`
    * operations using the given transformations.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def readFile[S[_]: Functor](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit
    S: ReadFileF :<: S
  ): S ~> S = {
    import ReadFile._

    val g = new (ReadFile ~> ReadFileF) {
      def apply[A](rf: ReadFile[A]) = rf match {
        case Open(src, off, lim) =>
          Coyoneda.lift(Open(inPath(src), off, lim))
            .map(_ leftMap transformErrorPath(outPath))

        case Read(h) =>
          Coyoneda.lift(Read(h))
            .map(_ leftMap transformErrorPath(outPath))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    injectedNT[ReadFileF, S](Coyoneda.liftTF(g))
  }

  /** Returns a natural transformation that transforms all paths in `WriteFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def writeFile[S[_]: Functor](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit
    S: WriteFileF :<: S
  ): S ~> S = {
    import WriteFile._

    val g = new (WriteFile ~> WriteFileF) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(dst) =>
          Coyoneda.lift(Open(inPath(dst)))
            .map(_ leftMap transformErrorPath(outPath))

        case Write(h, d) =>
          Coyoneda.lift(Write(h, d))
            .map(_ map transformErrorPath(outPath))

        case Close(h) =>
          Coyoneda.lift(Close(h))
      }
    }

    injectedNT[WriteFileF, S](Coyoneda.liftTF(g))
  }

  /** Returns a natural transformation that transforms all paths in `ManageFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    */
  def manageFile[S[_]: Functor](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath
  )(implicit S:
    ManageFileF :<: S
  ): S ~> S = {
    import ManageFile._, MoveScenario._

    val g = new (ManageFile ~> ManageFileF) {
      def apply[A](mf: ManageFile[A]) = mf match {
        case Move(scn, sem) =>
          Coyoneda.lift(Move(
            scn.fold(
              (src, dst) => DirToDir(inPath(src), inPath(dst)),
              (src, dst) => FileToFile(inPath(src), inPath(dst))),
            sem))
            .map(_ leftMap transformErrorPath(outPath))

        case Delete(p) =>
          Coyoneda.lift(Delete(inPath(p)))
            .map(_ leftMap transformErrorPath(outPath))

        case TempFile(p) =>
          Coyoneda.lift(TempFile(inPath(p)))
            .map(_ bimap (transformErrorPath(outPath), outPath(_)))
      }
    }

    injectedNT[ManageFileF, S](Coyoneda.liftTF(g))
  }

  /** Returns a natural transformation that transforms all paths in `QueryFile`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    * @param outPathR transforms relative output paths
    */
  def queryFile[S[_]: Functor](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath,
    outPathR: RelPath ~> RelPath
  )(implicit
    S: QueryFileF :<: S
  ): S ~> S = {
    import QueryFile._

    val g = new (QueryFile ~> QueryFileF) {
      def apply[A](qf: QueryFile[A]) = qf match {
        case ExecutePlan(lp, out) =>
          Coyoneda.lift(ExecutePlan(lp.translate(transformLPPaths(inPath)), inPath(out)))
            .map(_.map(_.bimap(transformErrorPath(outPath), outPath(_))))

        case EvaluatePlan(lp) =>
          Coyoneda.lift(EvaluatePlan(lp.translate(transformLPPaths(inPath))))
            .map(_.map(_ leftMap transformErrorPath(outPath)))

        case More(h) =>
          Coyoneda.lift(More(h))
            .map(_ leftMap transformErrorPath(outPath))

        case Close(h) =>
          Coyoneda.lift(Close(h))

        case Explain(lp) =>
          Coyoneda.lift(Explain(lp.translate(transformLPPaths(inPath))))
            .map(_.map(_ leftMap transformErrorPath(outPath)))

        case ListContents(d) =>
          Coyoneda.lift(ListContents(inPath(d)))
            .map(_.bimap(transformErrorPath(outPath), _ map transformNodePath(outPathR)))

        case FileExists(f) =>
          Coyoneda.lift(FileExists(inPath(f)))
            .map(_ leftMap transformErrorPath(outPath))
      }
    }

    injectedNT[QueryFileF, S](Coyoneda.liftTF(g))
  }

  /** Returns a natural transformation that transforms all paths in `FileSystem`
    * operations using the given functions.
    *
    * @param inPath transforms input paths
    * @param outPath transforms output paths (including those in errors)
    * @param outPathR transforms relative output paths
    */
  def fileSystem[S[_]: Functor](
    inPath: AbsPath ~> AbsPath,
    outPath: AbsPath ~> AbsPath,
    outPathR: RelPath ~> RelPath
  )(implicit
    S0: ReadFileF :<: S,
    S1: WriteFileF :<: S,
    S2: ManageFileF :<: S,
    S3: QueryFileF :<: S
  ): S ~> S = {
    readFile[S](inPath, outPath)   compose
    writeFile[S](inPath, outPath)  compose
    manageFile[S](inPath, outPath) compose
    queryFile[S](inPath, outPath, outPathR)
  }

  ////

  private val fsPathError: Optional[FileSystemError, APath] =
    FileSystemError.pathError composeLens PathError2.errorPath

  private val fsPlannerError: Optional[FileSystemError, Fix[LogicalPlan]] =
    FileSystemError.plannerError composeLens _1

  private def transformErrorPath(
    f: AbsPath ~> AbsPath
  ): FileSystemError => FileSystemError =
    fsPathError.modify(f(_)) compose
      fsPlannerError.modify(_ translate transformLPPaths(f))

  private def transformLPPaths(f: AbsPath ~> AbsPath): LogicalPlan ~> LogicalPlan =
    new (LogicalPlan ~> LogicalPlan) {
      def apply[A](lp: LogicalPlan[A]) = lp match {
        case ReadF(p) => ReadF(Path.fromAPath(f(p.asAPath)))
        case _        => lp
      }
    }

  private def transformNodePath(f: RelPath ~> RelPath): Node => Node =
    _.fold(
      Node.Mount compose (f(_)),
      Node.Plain compose (f(_)),
      Node.View  compose (f(_)))
}
