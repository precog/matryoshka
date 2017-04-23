/*
 * Copyright 2014–2017 SlamData Inc.
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
  implicit def nelBirecursive[A]
      : Birecursive.Aux[NonEmptyList[A], AndMaybe[A, ?]] =
    Birecursive.algebraIso({
      case Indeed(a, bs) => a <:: bs
      case Only(a)     => NonEmptyList(a)
    }, {
      case NonEmptyList(a, ICons(b, cs)) => Indeed(a, NonEmptyList.nel(b, cs))
      case NonEmptyList(a,       INil()) => Only(a)
    })
}

object nel extends NonEmptyListInstances
