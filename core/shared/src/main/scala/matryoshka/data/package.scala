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

import matryoshka.Recursive.ops._

import scalaz._, Scalaz._

package object data
    extends CofreeInstances
    with EitherInstances
    with FreeInstances
    with IdInstances
    with IListInstances
    with MaybeInstances {

  // final class CofParaMPartiallyApplied[M[_]] { self =>
  //   def apply[T[_[_]]: Corecursive, S[_]: Traverse, A, B](t: Cofree[S, A])(f: (A, S[(T[S], B)]) => M[B])(implicit M: Monad[M]): M[B] =
  //     t.tail.traverse(cs => self(cs)(f) ∘ ((Recursive[Cofree[?[_], A]].convertTo[S, T](cs), _))) >>= (f(t.head, _))
  // }

  /** NB: Since Cofree carries the functor, the resulting algebra is a cata, not
    *     a para.
    *
    * @group algtrans
    */
  def attributePara[T, F[_]: Functor, A]
    (f: GAlgebra[(T, ?), F, A])
    (implicit T: Corecursive.Aux[T, F])
      : Algebra[F, Cofree[F, A]] =
    fa => Cofree(f(fa ∘ (x => (x.cata[T](_.lower.embed), x.head))), fa)
}
