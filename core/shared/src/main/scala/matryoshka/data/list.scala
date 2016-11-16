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

import scala.collection.immutable.{::, List, Nil}

import scalaz._

trait ListInstances {
  implicit def listRecursive[A]: Recursive.Aux[List[A], ListF[A, ?]] =
    new Recursive[List[A]] {
      type Base[B] = ListF[A, B]

      def project(t: List[A])(implicit BF: Functor[Base]) = t match {
        case h :: t => ConsF(h, t)
        case Nil    => NilF[A, List[A]]()
      }
    }

  implicit def listCorecursive[A]: Corecursive.Aux[List[A], ListF[A, ?]] =
    new Corecursive[List[A]] {
      type Base[B] = ListF[A, B]

      def embed(t: ListF[A, List[A]])(implicit BF: Functor[Base]) = t match {
        case ConsF(h, t) => h :: t
        case NilF()      => Nil
      }
    }
}

object list extends ListInstances
