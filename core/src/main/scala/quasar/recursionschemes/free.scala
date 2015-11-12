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
package recursionschemes

object free {
  // TODO[scalaz-7.2]: Enable these once the `Functor` constraint is gone from `Free`

  // implicit def FreeTraverseT[A]: TraverseT[Free[?[_], A]] =
  //   new TraverseT[Free[?[_], A]] {
  //     def traverse[M[_]: Applicative, F[_], G[_]](t: Free[F, A])(f: F[Free[F, A]] => M[G[Free[G, A]]]) =
  //       t.fold(
  //         _.point[Free[F, ?]].point[M],
  //         f(_).map(Free.liftF(_).join))

  // implicit def FreeCorecursive[A]: Corecursive[Free [?[_], A]] =
  //   new Corecursive[Free [?[_], A]] {
  //     def embed[F[_]: Functor](t: F[Free[F, A]]) = Free.liftF(t).join
  //   }
}
