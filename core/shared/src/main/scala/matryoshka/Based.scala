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

import simulacrum.typeclass

/** Provides a type describing the pattern functor of some {co}recursive type
  * `T`. For standard fixed-point types like [[matryoshka.data.Fix]],
  * `Patterned[Fix[F]]#Base` is simply `F`. However, directly recursive types
  * generally have a less obivous pattern functor. E.g., `Patterned[Cofree[F,
  * A]]#Base` is `EnvT[A, F, ?]`.
  */
@typeclass trait Based[T] {
  type Base[A]
}
