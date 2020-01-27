/*
 * Copyright 2014–2019 SlamData Inc.
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

package matryoshka.data

import matryoshka._
import matryoshka.patterns._

import scalaz._

trait NonEmptyListInstances {
  implicit def nelRecursive[A]: Recursive.Aux[NonEmptyList[A], NelF[A, ?]] =
    new Recursive[NonEmptyList[A]] {
      type Base[B] = NelF[A, B]

      def project(t: NonEmptyList[A])(implicit BF: Functor[Base]) =
        t match {
          case NonEmptyList(a, ICons(b, cs)) => InitF(a, NonEmptyList.nel(b, cs))
          case NonEmptyList(a,       INil()) => LastF(a)
        }
    }

  implicit def nelCorecursive[A]: Corecursive.Aux[NonEmptyList[A], NelF[A, ?]] =
    new Corecursive[NonEmptyList[A]] {
      type Base[B] = NelF[A, B]

      def embed(b: NelF[A, NonEmptyList[A]])(implicit BF: Functor[Base]) =
        b match {
          case InitF(a, bs) => a <:: bs
          case LastF(a)     => NonEmptyList(a)
        }
    }
}

object nel extends NonEmptyListInstances
