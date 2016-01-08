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
import quasar.fp.TaskRef
import quasar.effect._

import scalaz._
import scalaz.concurrent.Task
import pathy.Path._

package object fs {
  type ReadFileF[A]    = Coyoneda[ReadFile, A]
  type WriteFileF[A]   = Coyoneda[WriteFile, A]
  type ManageFileF[A]  = Coyoneda[ManageFile, A]
  type QueryFileF[A]   = Coyoneda[QueryFile, A]
  type ViewStateF[A]   = Coyoneda[view.ViewState, A]

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

  def interpretFileSystem[M[_]: Functor](
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    interpret4[QueryFileF, ReadFileF, WriteFileF, ManageFileF, M](
      Coyoneda.liftTF(q), Coyoneda.liftTF(r), Coyoneda.liftTF(w), Coyoneda.liftTF(m))

  def interpretViewFileSystem[M[_]: Functor](
    v: view.ViewState ~> M,
    s: MonotonicSeq ~> M,
    fs: FileSystem ~> M
  ): ViewFileSystem ~> M =
    interpret3[ViewStateF, MonotonicSeqF, FileSystem, M](
      Coyoneda.liftTF(v), Coyoneda.liftTF(s), fs)

  // Remove once we have fully migrated to Pathy
  def convert(path: pathy.Path[_,_,Sandboxed]): fs.Path = fs.Path(posixCodec.printPath(path))

  // Remove once we have fully migrated to Pathy
  def convertToAFile(path: fs.Path): Option[AFile] = posixCodec.parseAbsFile(path.pathname) flatMap (sandbox(rootDir, _)) map (rootDir </> _)
}
