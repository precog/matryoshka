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

import matryoshka.implicits._

import scala.Some

import scalaz._

/** An extractor to make it easier to pattern-match on arbitrary [[Recursive]]
  * structures.
  *
  * NB: This extractor is irrufutable and doesn’t break exhaustiveness checking.
  */
object Embed {
  def unapply[T, F[_]](obj: T)(implicit T: Recursive.Aux[T, F], F: Functor[F])
      : Some[F[T]] =
    Some(obj.project)
}
