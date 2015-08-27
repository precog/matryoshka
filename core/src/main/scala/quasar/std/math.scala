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

trait MathLib extends Library {
  private val MathRel = (Type.Numeric | Type.Interval)
  private val MathAbs = (Type.Numeric | Type.Interval | Type.Temporal)

  private val biReflexiveUnapply: Func.Untyper = partialUntyperV {
    case Type.Const(d) => success(d.dataType :: d.dataType :: Nil)
    case t             => success(t          :: t          :: Nil)
  }

  /**
   * Adds two numeric values, promoting to decimal if either operand is decimal.
   */
  val Add = Mapping("(+)", "Adds two numeric or temporal values",
    MathAbs, MathAbs :: MathRel :: Nil,
    noSimplification,
    (partialTyper {
      case Type.Const(Data.Number(v1)) :: t2 :: Nil if (v1.signum == 0) && (Type.Numeric contains t2) => t2
      case t1 :: Type.Const(Data.Number(v2)) :: Nil if (Type.Numeric contains t1) && (v2.signum == 0) => t1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 + v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 + v2))

      case Type.Const(Data.Timestamp(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Timestamp(v1.plus(v2)))
      case Type.Timestamp :: Type.Interval :: Nil => Type.Timestamp
      case Type.Const(Data.Timestamp(_)) :: t2 :: Nil if t2 contains Type.Interval => Type.Timestamp
    }) ||| numericWidening,
    untyper(t => Type.typecheck(Type.Timestamp | Type.Interval, t).fold(
      κ(t match {
        case Type.Const(d) => success(d.dataType   :: d.dataType   :: Nil)
        case Type.Int      => success(Type.Int     :: Type.Int     :: Nil)
        case _             => success(Type.Numeric :: Type.Numeric :: Nil)
      }),
      κ(success(List(t, Type.Interval))))))

  /**
   * Multiplies two numeric values, promoting to decimal if either operand is decimal.
   */
  val Multiply = Mapping("(*)", "Multiplies two numeric values or one interval and one numeric value",
    MathRel, MathRel :: Type.Numeric :: Nil,
    noSimplification,
    (partialTyper {
      case (zero @ Type.Const(Data.Number(v1))) :: v2 :: Nil if (v1.signum == 0) => zero
      case v1 :: (zero @ Type.Const(Data.Number(v2))) :: Nil if (v2.signum == 0) => zero
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 * v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 * v2))

      // TODO: handle interval multiplied by Dec (not provided by threeten). See #580.
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Interval(v1.multipliedBy(v2.longValue)))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Interval(v2.multipliedBy(v1.longValue)))
      case Type.Const(Data.Interval(v1)) :: t :: Nil if t contains Type.Int => Type.Interval
    }) ||| numericWidening,
    biReflexiveUnapply)

  /**
   * Subtracts one value from another, promoting to decimal if either operand is decimal.
   */
  val Subtract = Mapping("(-)", "Subtracts two numeric or temporal values",
    MathAbs, MathAbs :: MathAbs :: Nil,
    noSimplification,
    (partialTyper {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.signum == 0) => v1
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 - v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 - v2))
      case Type.Timestamp :: Type.Timestamp :: Nil => Type.Interval
      case Type.Date :: Type.Date :: Nil => Type.Interval
      case Type.Time :: Type.Time :: Nil => Type.Interval
    }) ||| numericWidening,
    untyper(t => Type.typecheck(Type.Temporal, t).fold(
      κ(Type.typecheck(Type.Interval, t).fold(
        κ(t match {
          case Type.Const(d) => success(d.dataType   :: d.dataType   :: Nil)
          case Type.Int      => success(Type.Int     :: Type.Int     :: Nil)
          case _             => success(Type.Numeric :: Type.Numeric :: Nil)
        }),
        κ(success(List((Type.Temporal | Type.Interval), (Type.Temporal | Type.Interval)))))),
      κ(success(List(t, Type.Interval))))))

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = Mapping("(/)", "Divides one numeric or interval value by another (non-zero) numeric value",
    MathRel, MathAbs :: MathRel :: Nil,
    noSimplification,
    (partialTyperV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) => success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) => failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Int(v1 / v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => success(Type.Const(Data.Dec(v1 / v2)))

      // TODO: handle interval divided by Dec (not provided by threeten). See #580.
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Interval(v1.dividedBy(v2.longValue))))
      case Type.Const(Data.Interval(v1)) :: t :: Nil if t contains Type.Int => success(Type.Interval)
      case Type.Interval :: Type.Interval :: Nil => success(Type.Dec)
    }) ||| numericWidening,
    untyper(t => Type.typecheck(Type.Interval, t).fold(
      κ(t match {
        case Type.Const(d) => success(d.dataType :: d.dataType :: Nil)
        case Type.Int      => success(Type.Int   :: Type.Int   :: Nil)
        case _             => success(MathRel    :: MathRel    :: Nil)
      }),
      κ(success(List((Type.Temporal | Type.Interval), MathRel))))))

  /**
   * Aka "unary minus".
   */
  val Negate = Mapping("-", "Reverses the sign of a numeric or interval value",
    MathRel, MathRel :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Int(v)) :: Nil      => success(Type.Const(Data.Int(-v)))
      case Type.Const(Data.Number(v)) :: Nil   => success(Type.Const(Data.Dec(-v)))
      case Type.Const(Data.Interval(v)) :: Nil => success(Type.Const(Data.Interval(v.negated)))
      case t :: Nil if (Type.Numeric | Type.Interval) contains t => success(t)
    },
    untyper {
      case Type.Const(d) => success(d.dataType :: Nil)
      case t             => success(t          :: Nil)
    })

  val Modulo = Mapping(
    "(%)",
    "Finds the remainder of one number divided by another",
    MathRel, MathRel :: Type.Numeric :: Nil,
    noSimplification,
    (partialTyperV {
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 1.0) =>
        success(v1)
      case v1 :: Type.Const(Data.Number(v2)) :: Nil if (v2.doubleValue == 0.0) =>
        failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil =>
        success(Type.Const(Data.Int(v1 % v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil =>
        success(Type.Const(Data.Dec(v1 % v2)))
    }) ||| numericWidening,
    biReflexiveUnapply)

  def functions = Add :: Multiply :: Subtract :: Divide :: Negate :: Modulo :: Nil
}
object MathLib extends MathLib
