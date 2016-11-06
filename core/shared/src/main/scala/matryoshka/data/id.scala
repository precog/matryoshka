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

import scalaz._

trait IdInstances {
  /** This is a single (low-priority) instance to provide folds/unfolds for all
    * all non-recursive data types.
    */
  // NB: This should really be available even without an additional dependency,
  //     but [[scalaz.Const]] only exists in Scalaz (and Cats).
  def idRecursive[A]: Recursive.Aux[A, Const[A, ?]] = new Recursive[A] {
    type Base[B] = Const[A, B]

    def project(t: A)(implicit BF: Functor[Base]) = Const(t)
  }

  def idCorecursive[A]: Corecursive.Aux[A, Const[A, ?]] = new Corecursive[A] {
    type Base[B] = Const[A, B]

    def embed(t: Const[A, A])(implicit BF: Functor[Base]) = t.getConst
  }
}

object id extends IdInstances
