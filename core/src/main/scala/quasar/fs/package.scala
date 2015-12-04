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

import quasar.Predef._
import quasar.fp.free._
import quasar.effect._

import monocle.Lens
import pathy.{Path => PPath}, PPath._
import scalaz._, Id.Id
import scalaz.concurrent.Task

package object fs {
  type ReadFileF[A]    = Coyoneda[ReadFile, A]
  type WriteFileF[A]   = Coyoneda[WriteFile, A]
  type ManageFileF[A]  = Coyoneda[ManageFile, A]
  type QueryFileF[A]   = Coyoneda[QueryFile, A]

  type FileSystem0[A] = Coproduct[WriteFileF, ManageFileF, A]
  type FileSystem1[A] = Coproduct[ReadFileF, FileSystem0, A]
  /** FileSystem[A] = QueryFileF or ReadFileF or WriteFileF or ManageFileF */
  type FileSystem[A]  = Coproduct[QueryFileF, FileSystem1, A]

  type ViewFileSystem0[A] = Coproduct[MonotonicSeqF, FileSystem, A]
  /** Adds ViewStateF and MonotonicSeqF to FileSystem. */
  type ViewFileSystem[A]  = Coproduct[ViewStateF, ViewFileSystem0, A]

  type ADir  = AbsDir[Sandboxed]
  type RDir  = RelDir[Sandboxed]
  type AFile = AbsFile[Sandboxed]
  type RFile = RelFile[Sandboxed]
  type APath = pathy.Path[Abs,_,Sandboxed]
  type RPath = pathy.Path[Rel,_,Sandboxed]

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]
  type FileSystemErrT[F[_], A] = EitherT[F, FileSystemError, A]

  //-- Views --

  type ViewHandles = Map[
    ReadFile.ReadHandle,
    ReadFile.ReadHandle \/ QueryFile.ResultHandle]

  type ViewState[A] = KeyValueStore[
    ReadFile.ReadHandle,
    ReadFile.ReadHandle \/ QueryFile.ResultHandle,
    A]

  type ViewStateF[A] = Coyoneda[ViewState, A]

  object ViewState {
    def Ops[S[_]: Functor](
      implicit S: ViewStateF :<: S
    ): KeyValueStore.Ops[ReadFile.ReadHandle, ReadFile.ReadHandle \/ QueryFile.ResultHandle, S] =
      KeyValueStore.Ops[ReadFile.ReadHandle, ReadFile.ReadHandle \/ QueryFile.ResultHandle, S]

    def toTask(initial: ViewHandles): Task[ViewState ~> Task] =
      KeyValueStore.taskRefKeyValueStore(initial)

    def toState[S](l: Lens[S, ViewHandles]): ViewState ~> State[S, ?] =
      KeyValueStore.stateKeyValueStore[Id](l)
  }

  def interpretFileSystem[M[_]: Functor](
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    interpret4[QueryFileF, ReadFileF, WriteFileF, ManageFileF, M](
      Coyoneda.liftTF(q), Coyoneda.liftTF(r), Coyoneda.liftTF(w), Coyoneda.liftTF(m))

  def interpretViewFileSystem[M[_]: Functor](
    v: ViewState ~> M,
    s: MonotonicSeq ~> M,
    fs: FileSystem ~> M
  ): ViewFileSystem ~> M =
    interpret3[ViewStateF, MonotonicSeqF, FileSystem, M](
      Coyoneda.liftTF(v), Coyoneda.liftTF(s), fs)

  /** Rebases the given absolute path onto the provided absolute directory, so
    * `rebaseA(/foo/bar, /baz)` becomes `/baz/foo/bar`.
    */
  def rebaseA[T](apath: PPath[Abs,T,Sandboxed], onto: ADir): PPath[Abs,T,Sandboxed] =
    apath.relativeTo(rootDir[Sandboxed]).fold(apath)(onto </> _)

  /** Removes the given prefix from an `APath`, if present. */
  def stripAPathPrefix(prefix: ADir): APath => APath =
    p => stripPrefixA(prefix)(p)

  /** Removes the given prefix from an `RPath`, if present. */
  def stripRPathPrefix(prefix: ADir): RPath => RPath =
    p => stripPrefixR(prefix)(p)

  /** Removes the given prefix from a relative path, if present. */
  def stripPrefixR[T](prefix: ADir): PPath[Rel,T,Sandboxed] => PPath[Rel,T,Sandboxed] =
    p => prefix.relativeTo(rootDir).flatMap(p relativeTo _) getOrElse p

  /** Removes the given prefix from an absolute path, if present. */
  def stripPrefixA[T](prefix: ADir): PPath[Abs,T,Sandboxed] => PPath[Abs,T,Sandboxed] =
    p => p.relativeTo(prefix).fold(p)(rootDir </> _)

  // Remove once we have fully migrated to Pathy
  def convert(path: PPath[_,_,Sandboxed]): Path =
    Path(posixCodec.printPath(path))

  // Remove once we have fully migrated to Pathy
  def convertToAFile(path: Path): Option[AFile] =
    posixCodec.parseAbsFile(path.pathname) flatMap (sandbox(rootDir, _)) map (rootDir </> _)
}
