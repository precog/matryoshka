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
import quasar.fp._
import quasar.{Data, Func, Type, Mapping, SemanticError}, SemanticError._

import scalaz._, NonEmptyList.nel, Validation.{success, failure}

trait ArrayLib extends Library {
  val ArrayLength = Mapping(
    "array_length",
    "Gets the length of a given dimension of an array.",
    Type.AnyArray :: Type.Int :: Nil,
    noSimplification,
    partialTyperV {
      case _ :: Type.Const(Data.Int(dim)) :: Nil if (dim < 1) =>
        failure(nel(GenericError("array dimension out of range"), Nil))
      case Type.Const(Data.Arr(arr)) :: Type.Const(Data.Int(i)) :: Nil
          if (i == 1) =>
        // TODO: we should support dims other than 1, but it's work
        success(Type.Const(Data.Int(arr.length)))
      case Type.AnyArray :: Type.Const(Data.Int(_)) :: Nil =>
        success(Type.Int)
    },
    Type.typecheck(_, Type.Int) map κ(Type.AnyArray :: Type.Int :: Nil)
  )

  val In = Mapping(
    "(in)",
    "Determines whether a value is in a given array.",
    Type.Top :: Type.AnyArray :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(x), Type.Const(Data.Arr(arr))) =>
        Type.Const(Data.Bool(arr.contains(x)))
      case List(_,             Type.Const(Data.Arr(_)))   => Type.Bool
      case List(_,             _)                         => Type.Bool
    },
    Type.typecheck(_, Type.Bool) map κ(Type.Top :: Type.AnyArray :: Nil))

  def functions = ArrayLength :: In :: Nil
}
object ArrayLib extends ArrayLib
