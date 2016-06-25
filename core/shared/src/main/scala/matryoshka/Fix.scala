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

import scalaz._

/** This is the simplest fixpoint type, implemented with general recursion.
  */
final case class Fix[F[_]](unFix: F[Fix[F]])
object Fix {
  implicit val recursive: Recursive[Fix] = new Recursive[Fix] {
    def project[F[_]: Functor](t: Fix[F]) = t.unFix
  }

  implicit val corecursive: Corecursive[Fix] = new Corecursive[Fix] {
    def embed[F[_]: Functor](t: F[Fix[F]]) = Fix(t)
  }

  implicit def equal[F[_]](implicit F: Delay[Equal, F]): Equal[Fix[F]] =
    Equal.equal((a, b) => F(equal[F]).equal(a.unFix, b.unFix))

  implicit def show[F[_]](implicit F: Delay[Show, F]): Show[Fix[F]] =
    // NB: Doesn’t use Recursive.show in order to avoid the Functor constraint.
    Show.show(f => F(show[F]).show(f.unFix))
}
