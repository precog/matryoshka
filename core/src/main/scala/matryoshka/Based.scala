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

import scala.inline
import scala.Predef.implicitly

import scalaz._
import simulacrum.typeclass

/** Provides a type describing the “unwrapped” type of `T`. In the simplest
  * case, the `Base[F, A]` of `T[F]` is `F[A]`, but it may be more complicated
  * in others (cf. [[matryoshka.CofreeInstances]]).
  *
  * Basically, the standard fixed point types ([[matryoshka.Mu]],
  * [[matryoshka.Nu]], and [[matryoshka.Fix]]) have a base of `F[A]`, which
  * could be implemented without this type member. However, with `Base` we can
  * make non-fixed point types (Cofree, Free, List, etc.) behave _like_ fixed
  * point types by abstracting this type.
  *
  * For “true” fixed point types, `Base` is simplify the functor. E.g., The base
  * of `Nu[F]` is `F`. It exists in order to allow non-fixed point types to
  * pretend to be fixed point.
  */
@typeclass trait Based[T] {
  type Base[A]

  implicit def BF: Functor[Base] = implicitly
}
