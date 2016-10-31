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

package matryoshka.helpers

import matryoshka._, data._

import java.lang.String

import monocle.law.discipline._
import org.scalacheck._
import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._

trait AlgebraChecks extends SpecificationLike with Discipline {
  def checkFoldIsoLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Functor, A](
    name: String, iso: AlgebraIso[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[A]) =
    checkAll(name + " Iso", IsoTests(foldIso[T, F, A](iso)))

  def checkFoldPrismLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Traverse, A](
    name: String, prism: AlgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[A]) =
    checkAll(name + " Prism", PrismTests(foldPrism(prism)))

  def checkUnfoldPrismLaws[T[_[_]]: Recursive: Corecursive: EqualT, F[_]: Traverse, A](
    name: String, prism: CoalgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[T[F]]) =
    checkAll(name + " Prism", PrismTests(unfoldPrism(prism)))

  def checkAlgebraIsoLaws[F[_], A](
    name: String, iso: AlgebraIso[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[A]) =
    checkAll(name + " Iso", IsoTests(iso))

  def checkAlgebraPrismLaws[F[_], A](
    name: String, prism: AlgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[A]) =
    checkAll(name + " Prism", PrismTests(prism))

  def checkCoalgebraPrismLaws[F[_], A](
    name: String, prism: CoalgebraPrism[F, A])(
    implicit FA: Delay[Arbitrary, F], AA: Arbitrary[A], FE: Delay[Equal, F], AE: Equal[A], CO: Cogen[F[A]]) =
    checkAll(name + " Prism", PrismTests(prism))
}
