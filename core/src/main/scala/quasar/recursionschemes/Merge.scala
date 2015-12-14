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

// TODO: This really deserves to be in something like Scalaz
package quasar.recursionschemes

@typeclass trait Merge[F[_]] { self =>
  def merge[A, B](fa: => F[A], fb: => F[B]): Option[F[(A, B)]]

  def mergeWith[A, B, C](
    fa: => F[A], fb: => F[B])(
    f: (F[A], F[B]) \/ (A, B) => C)(
    implicit F: Functor[F]):
      C \/ F[C] =
    merge(fa, fb) match {
      case None         => f((fa, fb).left).left
      case Some(merged) => merged.map(x => f(x.right)).right
    }
}

object Merge {
  implicit def MergeTraverse[F[_]: Traverse] = new Merge[F] {
    def merge[A, B](fa: => F[A], fb: => F[B]): Option[F[(A, B)]] =
      if (fa.void == fb.void)
        fa.zipWithL(fb)((a, b) => b.map((a, _))).sequence
      else None
  }
}

