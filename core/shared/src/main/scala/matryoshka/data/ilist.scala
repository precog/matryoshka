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
import matryoshka.patterns._

import scalaz._

trait IListInstances {
  implicit def ilistRecursive[A]: Recursive.Aux[IList[A], ListF[A, ?]] =
    new Recursive[IList[A]] {
      type Base[B] = ListF[A, B]

      def project(t: IList[A])(implicit BF: Functor[Base]) = t match {
        case ICons(h, t) => ConsF(h, t)
        case INil()      => NilF[A, IList[A]]()
      }
    }

  implicit def ilistCorecursive[A]: Corecursive.Aux[IList[A], ListF[A, ?]] =
    new Corecursive[IList[A]] {
      type Base[B] = ListF[A, B]

      def embed(t: ListF[A, IList[A]])(implicit BF: Functor[Base]) = t match {
        case ConsF(h, t) => ICons(h, t)
        case NilF()      => INil[A]
      }
    }
}

object ilist extends IListInstances
