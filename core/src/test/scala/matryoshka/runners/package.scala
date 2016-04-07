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

import Recursive.ops._

import scala.Unit
import scala.Predef.implicitly

import scalaz._, Scalaz._

import org.specs2.execute._
import org.specs2.matcher._
import org.specs2.mutable.SpecificationLike

package object runners extends SpecificationLike {

  // Extracted from scalaz 7.1.x. Was removed in scalaz 7.2.0 because `~>` is not a typeclass but it's replacement
  // `Eq1` is only defined in tests. We only use it for tests as well, so sticking it here for now.
  // TODO: Consider using the version in `shapeless-scalaz` although that would require pulling in all of shapeless just
  // for this.
  implicit def cofreeEqual[A, F[_]](implicit A: Equal[A], F: Equal ~> λ[α => Equal[F[α]]]): Equal[Cofree[F, A]] =
    Equal.equal{ (a, b) =>
      A.equal(a.head, b.head) && F(cofreeEqual[A, F]).equal(a.tail, b.tail)
    }

  abstract class RecRunner[F[_], A] {
    // NB: This is defined as a function to make the many definition sites
    //     slightly shorter.
    def run[T[_[_]]: Recursive]: T[F] => MatchResult[A]
  }
  def testRec[F[_], A](t: Fix[F], r: RecRunner[F, A])(implicit F: Functor[F]):
      MatchResult[A] = {
    r.run[Fix].apply(t) and
    r.run[Mu].apply(t.convertTo[Mu]) and
    r.run[Nu].apply(t.convertTo[Nu]) and
    r.run[Cofree[?[_], Unit]].apply(t.convertTo[Cofree[?[_], Unit]](F, cofreeCorecursive[Unit]))
  }

  abstract class CorecRunner[M[_], F[_], A] {
    def run[T[_[_]]: Corecursive](implicit Eq: Equal[T[F]], S: Show[T[F]]):
        A => MatchResult[M[T[F]]]
  }
  def testCorec[M[_], F[_]: Functor, A](
    a: A, r: CorecRunner[M, F, A])(
    implicit Eq0: Equal ~> λ[α => Equal[F[α]]], S0: Show ~> λ[α => Show[F[α]]]):
      Result =
    r.run[Fix].apply(a).toResult and
    r.run[Mu].apply(a).toResult and
    r.run[Nu].apply(a).toResult and
    // NB: No Equal for Free, so we can’t test it.
    r.run[Cofree[?[_], Unit]](cofreeCorecursive[Unit], implicitly, implicitly).apply(a).toResult

  abstract class FuncRunner[F[_], G[_]] {
    def run[T[_[_]]: FunctorT: Corecursive](implicit Eq: Equal[T[G]], S: Show[T[G]]):
        T[F] => MatchResult[T[G]]
  }
  def testFunc[F[_], G[_]: Functor](
    t: Fix[F], r: FuncRunner[F, G])(
    implicit F: Functor[F], Eq0: Equal ~> λ[α => Equal[G[α]]], S0: Show ~> λ[α => Show[G[α]]]):
      Result =
    r.run[Fix].apply(t).toResult and
    r.run[Mu].apply(t.convertTo[Mu]).toResult and
    r.run[Nu].apply(t.convertTo[Nu]).toResult and
    // NB: No Equal for Free, so we can’t test it.
    r.run[Cofree[?[_], Unit]].apply(t.convertTo[Cofree[?[_], Unit]](F, cofreeCorecursive[Unit])).toResult

  abstract class TravRunner[M[_], F[_], G[_]] {
    def run[T[_[_]]: TraverseT: Corecursive](implicit Eq: Equal[T[G]], S: Show[T[G]]):
        T[F] => MatchResult[M[T[G]]]
  }
  def testTrav[M[_], F[_], G[_]: Functor](
    t: Fix[F], r: TravRunner[M, F, G])(
    implicit F: Functor[F], Eq0: Equal ~> λ[α => Equal[G[α]]], S0: Show ~> λ[α => Show[G[α]]]):
      Result =
    r.run[Fix].apply(t).toResult and
    r.run[Mu].apply(t.convertTo[Mu]).toResult and
    r.run[Nu].apply(t.convertTo[Nu]).toResult and
    // NB: No Equal for Free, so we can’t test it.
    r.run[Cofree[?[_], Unit]].apply(t.convertTo[Cofree[?[_], Unit]](F, cofreeCorecursive[Unit])).toResult

}
