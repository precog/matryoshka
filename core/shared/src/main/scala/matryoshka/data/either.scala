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

package matryoshka.data

import matryoshka._

import scala.Either

import scalaz._

trait EitherInstances {
  implicit def eitherRecursive[A, B]: Recursive.Aux[Either[A, B], Const[Either[A, B], ?]] =
    id.idRecursive[Either[A, B]]

  implicit def eitherCorecursive[A, B]: Corecursive.Aux[Either[A, B], Const[Either[A, B], ?]] =
    id.idCorecursive[Either[A, B]]
}

object either extends EitherInstances
