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

import scalaz._

/** To avoid diverging implicits with fixed-point types, we need to defer the
  * lookup.
  */
trait Delay[F[_], G[_]] {
  def apply[A](fa: F[A]): F[G[A]]
}

object Delay {
  /** Delay used to be a type alias for a natural transformation:
    *   type Delay[F[_], G[_]] = F ~> (F ∘ G)#λ
    * As an interim measure, this lifts natural transformations of the above
    * form into the Delay type class. But the end goal is to be unconnected to
    * NaturalTransformation.
    */
  def fromNT[F[_], G[_]](nt: F ~> (F ∘ G)#λ): Delay[F, G] =
    new Delay[F, G] { def apply[A](fa: F[A]): F[G[A]] = nt(fa) }

  def apply[F[_], G[_]](implicit ev: Delay[F, G]): Delay[F, G] = ev
}
