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
import quasar.recursionschemes._
import quasar._, LogicalPlan._

import scalaz._, NonEmptyList.nel, Validation.{success, failure}
import scalaz.syntax.applicative._

trait SetLib extends Library {
  val Take = Sifting("TAKE", "Takes the first N elements from a set", Type.Top :: Type.Int :: Nil,
    noSimplification,
    partialTyper {
      case _ :: Type.Const(Data.Int(n)) :: Nil if n == 0 =>
        Type.Const(Data.Set(Nil))
      case Type.Set(t) :: _ :: Nil => t
      case t           :: _ :: Nil => t
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Int :: Nil)
      case t           => success(Type.Set(t) :: Type.Int :: Nil)
    })

  val Drop = Sifting("DROP", "Drops the first N elements from a set", Type.Top :: Type.Int :: Nil,
    partialSimplifier {
      case List(set, Fix(ConstantF(Data.Int(n)))) if n == 0 => set
    },
    partialTyper {
      case Type.Set(t) :: _ :: Nil => t
      case t           :: _ :: Nil => t
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Int :: Nil)
      case t           => success(Type.Set(t) :: Type.Int :: Nil)
    })

  val OrderBy = Sifting("ORDER BY", "Orders a set by the natural ordering of a projection on the set", Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case set :: by :: Nil => set
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Top :: Nil)
      case t           => success(Type.Set(t) :: Type.Top :: Nil)
    })

  val Filter = Sifting("WHERE", "Filters a set to include only elements where a projection is true", Type.Top :: Type.Bool :: Nil,
    partialSimplifier {
      case List(set, Fix(ConstantF(Data.True))) => set
    },
    partialTyper {
      case _   :: Type.Const(Data.False) :: Nil => Type.Const(Data.Set(Nil))
      case set :: by                     :: Nil => set
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Bool :: Nil)
      case t           => success(Type.Set(t) :: Type.Bool :: Nil)
    })

  val InnerJoin = Transformation(
    "INNER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition.",
    Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    partialTyper {
      case List(_, _, Type.Const(Data.Bool(false))) => Type.Const(Data.Set(Nil))
      case List(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case List(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) => Type.Obj(Map("left" -> s1, "right" -> s2), None)
    },
    t => (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))(_ :: _ :: Type.Bool :: Nil))

  val LeftOuterJoin = Transformation(
    "LEFT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the left set.",
    Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    partialTyper {
      case List(s1, _, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> s1, "right" -> Type.Null), None)
      case List(Type.Const(Data.Set(Nil)), _, _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) =>
        Type.Obj(Map("left" -> s1, "right" -> (s2 | Type.Null)), None)
    },
    t => (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))(_ :: _ :: Type.Bool :: Nil))

  val RightOuterJoin = Transformation(
    "RIGHT OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from the right set.",
    Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    partialTyper {
      case List(_, s2, Type.Const(Data.Bool(false))) =>
        Type.Obj(Map("left" -> Type.Null, "right" -> s2), None)
      case List(_, Type.Const(Data.Set(Nil)), _) => Type.Const(Data.Set(Nil))
      case List(s1, s2, _) => Type.Obj(Map("left" -> (s1 | Type.Null), "right" -> s2), None)
    },
    t => (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))(_ :: _ :: Type.Bool :: Nil))

  val FullOuterJoin = Transformation(
    "FULL OUTER JOIN",
    "Returns a new set containing the pairs values from the two sets that satisfy the condition, plus all other values from either set.",
    Type.Top :: Type.Top :: Type.Bool :: Nil,
    noSimplification,
    partialTyper {
      case List(Type.Const(Data.Set(Nil)), Type.Const(Data.Set(Nil)), _) =>
        Type.Const(Data.Set(Nil))
      case List(s1, s2, _) =>
        Type.Obj(Map("left" -> (s1 | Type.Null), "right" -> (s2 | Type.Null)), None)
    },
    t => (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))(_ :: _ :: Type.Bool :: Nil))

  val GroupBy = Transformation("GROUP BY", "Groups a projection of a set by another projection", Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case s1 :: s2 :: Nil => s1
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Top :: Nil)
      case t           => success(Type.Set(t) :: Type.Top :: Nil)
    })

  val Distinct = Sifting("DISTINCT", "Discards all but the first instance of each unique value",
    Type.Top :: Nil,
    noSimplification,
    partialTyper { case a :: Nil => a},
    x => success(x :: Nil))

  val DistinctBy = Sifting("DISTINCT BY", "Discards all but the first instance of the first argument, based on uniqueness of the second argument",
    Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper { case a :: _ :: Nil => a },
    x => success(x :: Type.Top :: Nil))

  def functions = Take :: Drop :: OrderBy :: Filter :: InnerJoin :: LeftOuterJoin :: RightOuterJoin :: FullOuterJoin :: GroupBy :: Distinct :: DistinctBy :: Nil
}
object SetLib extends SetLib
