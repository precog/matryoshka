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

package matryoshka.instances.std

import matryoshka._
import matryoshka.instances.scalaz.id

import scala.{Either, Option}
import scala.collection.immutable.{::, List, Nil}

trait OptionInstances {
  implicit def optionMatryoshka[A]:
      Recursive[Option[A]] with Corecursive[Option[A]] =
    id.idMatryoshka[Option[A]]
}

object option extends OptionInstances

trait EitherInstances {
  implicit def eitherMatryoshka[A, B]:
      Recursive[Either[A, B]] with Corecursive[Either[A, B]] =
    id.idMatryoshka[Either[A, B]]
}

object either extends EitherInstances

trait ListInstances {
  implicit def listMatryoshka[A]:
      Recursive[List[A]] with Corecursive[List[A]] =
    new Recursive[List[A]] with Corecursive[List[A]] {
      type Base[B] = ListF[A, B]
      def project(t: List[A]) = t match {
        case h :: t => ConsF(h, t)
        case Nil    => NilF[A, List[A]]()
      }
      def embed(t: ListF[A, List[A]]) = t match {
        case ConsF(h, t) => h :: t
        case NilF()      => Nil
      }
    }
}

object list extends ListInstances
