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

package quasar.effect

import quasar.Predef._
import quasar.fp.TaskRef

import monocle.Lens
import scalaz.{Lens => _, _}, Scalaz._
import scalaz.concurrent.Task

/** Provides the ability to read, write and delete from a store of values
  * indexed by keys.
  *
  * @tparam K the type of keys used to index values
  * @tparam V the type of values in the store
  */
sealed trait KeyValueStore[K, V, A]

object KeyValueStore {
  final case class Get[K, V](k: K)
    extends KeyValueStore[K, V, Option[V]]

  final case class Put[K, V](k: K, v: V)
    extends KeyValueStore[K, V, Unit]

  final case class Delete[K, V](k: K)
    extends KeyValueStore[K, V, Unit]

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[K, V, S[_]: Functor](implicit S: KeyValueStoreF[K, V, ?] :<: S)
    extends LiftedOps[KeyValueStore[K, V, ?], S] {

    def get(k: K): OptionT[F, V] =
      OptionT(lift(Get[K, V](k)))

    def put(k: K, v: V): F[Unit] =
      lift(Put(k, v))

    def delete(k: K): F[Unit] =
      lift(Delete(k))

    def modify(k: K, f: V => V): F[Unit] =
      get(k) flatMapF (v => put(k, f(v))) getOrElse (())
  }

  object Ops {
    def apply[K, V, S[_]: Functor](implicit S: KeyValueStoreF[K, V, ?] :<: S): Ops[K, V, S] =
      new Ops[K, V, S]
  }

  def taskRefKeyValueStore[A, B](initial: Map[A, B]): Task[KeyValueStore[A, B, ?] ~> Task] = {
    TaskRef(initial).map(ref =>
      new (KeyValueStore[A, B, ?] ~> Task) {
        def apply[C](fa: KeyValueStore[A, B, C]): Task[C] = fa match {
          case Put(key, value) =>
            ref.modifyS(m => (m + (key -> value), ()))
          case Get(key) =>
            ref.read.map(_.get(key))
          case Delete(key) =>
            ref.modifyS(m => ((m - key), ()))
        }
      })
  }

  def stateKeyValueStore[F[_]: Monad, K, V, S](l: Lens[S, Map[K, V]]) =
    new (KeyValueStore[K, V, ?] ~> StateT[F, S, ?]) {
      def apply[A](fa: KeyValueStore[K, V, A]): StateT[F, S, A] = fa match {
        case Put(key, value) =>
          StateT[F, S, A](s => (l.modify(_ + (key -> value))(s), ()).point[F])

        case Get(key) =>
          StateT.stateTMonadState[S, F].gets(s => l.get(s).get(key))

        case Delete(key) =>
          StateT.stateTMonadState[S, F].modify(s => l.modify(_ - key)(s))
      }
    }
}
