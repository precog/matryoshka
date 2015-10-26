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
import quasar.{Data, Func, LogicalPlan, Type, Mapping, SemanticError}, LogicalPlan._, SemanticError._

import quasar.recursionschemes._

import scalaz._, Scalaz._, NonEmptyList.nel, Validation.{success, failure}

trait StringLib extends Library {
  private def stringApply(f: (String, String) => String): Func.Typer =
    partialTyper {
      case Type.Const(Data.Str(a)) :: Type.Const(Data.Str(b)) :: Nil => Type.Const(Data.Str(f(a, b)))

      case Type.Str :: Type.Const(Data.Str(_)) :: Nil => Type.Str
      case Type.Const(Data.Str(_)) :: Type.Str :: Nil => Type.Str
      case Type.Str :: Type.Str :: Nil                => Type.Str
    }

  // TODO: variable arity
  val Concat = Mapping("concat", "Concatenates two (or more) string values",
    Type.Str, Type.Str :: Type.Str :: Nil,
    partialSimplifier {
      case List(Fix(ConstantF(Data.Str(""))), other) => other
      case List(other, Fix(ConstantF(Data.Str("")))) => other
    },
    stringApply(_ + _),
    basicUntyper)

  val Like = Mapping(
    "(like)",
    "Determines if a string value matches a pattern.",
    Type.Bool, Type.Str :: Type.Str :: Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case List(Type.Str, Type.Const(Data.Str(_)), Type.Const(Data.Str(_))) =>
        success(Type.Bool)
      case Type.Str :: _ :: _ :: Nil =>
        failure(nel(GenericError("expected string constant for LIKE"), Nil))
    },
    basicUntyper)

  def matchAnywhere(str: String, pattern: String) = java.util.regex.Pattern.compile(pattern).matcher(str).find()

  val Search = Mapping(
    "search",
    "Determines if a string value matches a regular expresssion.",
    Type.Bool, Type.Str :: Type.Str :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Str(str)) :: Type.Const(Data.Str(pattern)) :: Nil =>
        success(Type.Const(Data.Bool(matchAnywhere(str, pattern))))
      case strT :: patternT :: Nil =>
        (Type.typecheck(Type.Str, strT) |@| Type.typecheck(Type.Str, patternT))((_, _) => Type.Bool)
    },
    basicUntyper)

  val Length = Mapping(
    "length",
    "Counts the number of characters in a string.",
    Type.Int, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil => Type.Const(Data.Int(str.length))
      case Type.Str :: Nil                  => Type.Int
    },
    basicUntyper)

  val Lower = Mapping(
    "lower",
    "Converts the string to lower case.",
    Type.Str, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil =>
        Type.Const(Data.Str(str.toLowerCase))
      case Type.Str :: Nil => Type.Str
    },
    basicUntyper)

  val Upper = Mapping(
    "upper",
    "Converts the string to upper case.",
    Type.Str, Type.Str :: Nil,
    noSimplification,
    partialTyper {
      case Type.Const(Data.Str(str)) :: Nil =>
        Type.Const (Data.Str(str.toUpperCase))
      case Type.Str :: Nil => Type.Str
    },
    basicUntyper)

  val Substring: Mapping = Mapping(
    "substring",
    "Extracts a portion of the string",
    Type.Str, Type.Str :: Type.Int :: Type.Int :: Nil,
    partialSimplifier {
      case List(Fix(ConstantF(Data.Str(str))), Fix(ConstantF(Data.Int(from))), for0) if 0 < from =>
        Substring(
          Constant(Data.Str(str.substring(from.intValue))),
          Constant(Data.Int(0)),
          for0)
    },
    partialTyperV {
      case List(
        Type.Const(Data.Str(str)),
        Type.Const(Data.Int(from)),
        Type.Const(Data.Int(for0))) => {
        success(Type.Const(Data.Str(str.substring(from.intValue, from.intValue + for0.intValue))))
      }
      case List(Type.Const(Data.Str(str)), Type.Const(Data.Int(from)), _)
          if str.length <= from =>
        success(Type.Const(Data.Str("")))
      case List(Type.Const(Data.Str(_)), Type.Const(Data.Int(_)), Type.Int) =>
        success(Type.Str)
      case List(Type.Const(Data.Str(_)), Type.Int, Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Const(Data.Str(_)), Type.Int,                Type.Int) =>
        success(Type.Str)
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Int)                =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Int)                =>
        success(Type.Str)
      case List(Type.Str, _,                       _)                       =>
        failure(nel(GenericError("expected integer arguments for SUBSTRING"), Nil))
      case List(t, _, _) => failure(nel(TypeError(Type.Str, t, None), Nil))
    },
    basicUntyper)

  def functions = Concat :: Like :: Search :: Length :: Lower :: Upper :: Substring :: Nil
}
object StringLib extends StringLib
