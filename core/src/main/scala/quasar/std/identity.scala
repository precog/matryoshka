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

package quasar.std

import quasar.Predef._

import scalaz._
import quasar.fp._
import quasar._

trait IdentityLib extends Library {
  import Type._
  import Validation.{success, failure}

  val Squash = Squashing("SQUASH", "Squashes all dimensional information", Top :: Nil,
    noSimplification,
    partialTyper { case x :: Nil => x },
    tpe => success(tpe :: Nil))

  val ToId = Mapping(
    "oid",
    "Converts a string to a (backend-specific) object identifier.",
    Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil => Type.Const(Data.Id(str))
      case Type.Str :: Nil                  => Type.Id
    },
    Type.typecheck(_, Type.Id) map κ(Type.Str :: Nil))

  val functions = Squash :: ToId :: Nil
}
object IdentityLib extends IdentityLib
