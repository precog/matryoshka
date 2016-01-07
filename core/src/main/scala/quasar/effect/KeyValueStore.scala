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

import scalaz._

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
}
