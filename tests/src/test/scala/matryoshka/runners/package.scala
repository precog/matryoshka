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

import matryoshka.data._
import matryoshka.implicits._

import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.mutable.SpecificationLike
import scalaz._, Scalaz._

package object runners extends SpecificationLike {

  abstract class RecRunner[F[_], A] {
    // NB: This is defined as a function to make the many definition sites
    //     slightly shorter.
    def run[T](implicit T: Recursive.Aux[T, F]): T => MatchResult[A]
  }
  def testRec[F[_], A](t: Fix[F], r: RecRunner[F, A])(implicit F: Functor[F]):
      MatchResult[A] = {
    r.run[Fix[F]].apply(t) and
    r.run[Mu[F]].apply(t.convertTo[Mu[F]]) and
    r.run[Nu[F]].apply(t.convertTo[Nu[F]])
  }

  abstract class CorecRunner[M[_], F[_], A] {
    def run[T](implicit TC: Corecursive.Aux[T, F], Eq: Equal[T], S: Show[T]):
      A => MatchResult[M[T]]
  }
  def testCorec[M[_], F[_]: Functor, A](
    a: A, r: CorecRunner[M, F, A])(
    implicit Eq0: Delay[Equal, F], S0: Delay[Show, F]):
      Result =
    r.run[Fix[F]].apply(a).toResult and
    r.run[Mu[F]].apply(a).toResult and
    r.run[Nu[F]].apply(a).toResult

  abstract class FuncRunner[F[_], G[_]] {
    def run[T[_[_]]: FunctorT](implicit TC: Corecursive.Aux[T[G], G], Eq: Equal[T[G]], S: Show[T[G]]):
      T[F] => MatchResult[T[G]]
  }
  def testFunc[F[_]: Functor, G[_]: Functor](
    t: Fix[F], r: FuncRunner[F, G])(
    implicit Eq0: Delay[Equal, G], S0: Delay[Show, G]):
      Result =
    r.run[Fix].apply(t).toResult and
    r.run[Mu].apply(t.convertTo[Mu[F]]).toResult and
    r.run[Nu].apply(t.convertTo[Nu[F]]).toResult

  abstract class TravRunner[M[_], F[_], G[_]] {
    def run[T[_[_]]: TraverseT](implicit TC: Corecursive.Aux[T[G], G], Eq: Equal[T[G]], S: Show[T[G]]):
      T[F] => MatchResult[M[T[G]]]
  }
  def testTrav[M[_], F[_]: Functor, G[_]: Functor](
    t: Fix[F], r: TravRunner[M, F, G])(
    implicit Eq0: Delay[Equal, G], S0: Delay[Show, G]):
      Result =
    r.run[Fix].apply(t).toResult and
    r.run[Mu].apply(t.convertTo[Mu[F]]).toResult and
    r.run[Nu].apply(t.convertTo[Nu[F]]).toResult
}
