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

import matryoshka._
import matryoshka.scalacheck.arbitrary._

import java.lang.String

import monocle.law.discipline._
import org.scalacheck._
import org.specs2.mutable._
import org.typelevel.discipline.specs2.mutable._
import scalaz._, Scalaz._

trait AlgebraChecks extends SpecificationLike with Discipline {
  def checkFoldIsoLaws[T: Arbitrary: Equal, F[_]: Functor, A: Arbitrary: Equal]
    (name: String, iso: AlgebraIso[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    checkAll(name + " Iso", IsoTests(foldIso[T, F, A](iso)))

  def checkFoldPrismLaws
    [T: Arbitrary: Equal, F[_]: Traverse, A: Arbitrary: Equal]
    (name: String, prism: AlgebraPrism[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    checkAll(name + " Prism", PrismTests(foldPrism(prism)))

  def checkUnfoldPrismLaws
    [T: Arbitrary: Equal, F[_]: Traverse, A: Arbitrary: Equal]
    (name: String, prism: CoalgebraPrism[F, A])
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F]) =
    checkAll(name + " Prism", PrismTests(unfoldPrism(prism)))

  def checkAlgebraIsoLaws[F[_], A: Arbitrary: Equal]
    (name: String, iso: AlgebraIso[F, A])
    (implicit FA: Delay[Arbitrary, F], FE: Delay[Equal, F]) =
    checkAll(name + " Iso", IsoTests(iso))

  def checkAlgebraPrismLaws[F[_], A: Arbitrary: Equal]
    (name: String, prism: AlgebraPrism[F, A])
    (implicit FA: Delay[Arbitrary, F], FE: Delay[Equal, F]) =
    checkAll(name + " Prism", PrismTests(prism))

  def checkCoalgebraPrismLaws[F[_], A: Arbitrary: Equal]
    (name: String, prism: CoalgebraPrism[F, A])
    (implicit FA: Delay[Arbitrary, F], FE: Delay[Equal, F]) =
    checkAll(name + " Prism", PrismTests(prism))
}
