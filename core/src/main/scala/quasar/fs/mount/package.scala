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

import quasar.Predef.Map
import quasar.effect._
import quasar.fp.free

import monocle.Lens
import scalaz.{Lens => _, _}, Id.Id
import scalaz.concurrent.Task

package object mount {
  type MountingF[A] = Coyoneda[Mounting, A]
  type MntErrT[F[_], A] = EitherT[F, MountingError, A]

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

  type ViewFileSystem0[A] = Coproduct[MonotonicSeqF, FileSystem, A]
  /** Adds ViewStateF and MonotonicSeqF to FileSystem. */
  type ViewFileSystem[A]  = Coproduct[ViewStateF, ViewFileSystem0, A]

  def interpretViewFileSystem[M[_]: Functor](
    v: ViewState ~> M,
    s: MonotonicSeq ~> M,
    fs: FileSystem ~> M
  ): ViewFileSystem ~> M =
    free.interpret3[ViewStateF, MonotonicSeqF, FileSystem, M](
      Coyoneda.liftTF(v), Coyoneda.liftTF(s), fs)
}
