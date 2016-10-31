/*
 * Copyright 2014–2016 SlamData Inc.
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

import scala.{None, Option, Unit}
import simulacrum._
import scalaz._, Scalaz._

/** Like `Zip`, but it can fail to merge, so it’s much more general.
  */
@typeclass trait Merge[F[_]] {
  // TODO[simulacrum#57]: `fa` should be lazy
  def merge[A, B](fa: F[A], fb: => F[B]): Option[F[(A, B)]]

  // TODO[simulacrum#57]: `fa` should be lazy
  def mergeWith[A, B, C, D](
    fa: F[A], fb: => F[B])(g: (A, B) => D)(
    implicit F: Functor[F]):
      Option[F[D]] =
    merge(fa, fb).map(_ ∘ g.tupled)
}

object Merge {
  implicit def fromTraverse[F[_]: Traverse](implicit E: Equal[F[Unit]]):
      Merge[F] =
    new Merge[F] {
      def merge[A, B](fa: F[A], fb: => F[B]): Option[F[(A, B)]] =
        if (fa.void ≟ fb.void)
          fa.zipWithL(fb)((a, b) => b ∘ ((a, _))).sequence
        else None
    }
}
