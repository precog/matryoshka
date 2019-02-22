/*
 * Copyright 2014–2018 SlamData Inc.
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

package matryoshka

import slamdata.Predef._

import scalaz._

/** This is a workaround for a certain use case (e.g.,
  * [[matryoshka.patterns.Diff]] and [[matryoshka.patterns.PotentialFailure]]).
  * Define an instance of this rather than [[Recursive]] when possible.
  */
// NB: Not a `@typeclass` because we don’t want to inject these operations.
// Changed Serializable because Quasar connector for Apache Spark needs it to be Serializable
// We can allow above since this trait will be removed once we have mutual recursion
trait RecursiveT[T[_[_]]] extends Serializable {
  def projectT[F[_]: Functor](t: T[F]): F[T[F]]

  def cataT[F[_]: Functor, A](t: T[F])(f: Algebra[F, A]): A =
    hylo(t)(f, projectT[F])
}

object RecursiveT {
  def apply[T[_[_]]](implicit instance: RecursiveT[T]) = instance
}
