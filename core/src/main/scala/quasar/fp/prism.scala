/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.fp

import monocle.Prism
import scalaz._

trait PrismInstances {
  import Liskov.<~<

  // TODO: See if we can implement this once for all tuples using shapeless.
  implicit class PrismOps[A, B](prism: Prism[A, B]) {
    def apply(b: B): A = prism.reverseGet(b)

    def apply[C, D](c: C, d: D)(implicit ev: (C, D) <~< B): A =
      apply(ev((c, d)))

    def apply[C, D, E](c: C, d: D, e: E)(implicit ev: (C, D, E) <~< B): A =
      apply(ev((c, d, e)))

    def apply[C, D, E, F](c: C, d: D, e: E, f: F)(implicit ev: (C, D, E, F) <~< B): A =
      apply(ev((c, d, e, f)))
  }
}

object prism extends PrismInstances
