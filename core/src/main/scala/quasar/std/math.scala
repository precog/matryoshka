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
import quasar.recursionschemes._, Recursive.ops._
import quasar.{Data, Func, LogicalPlan, Type, Mapping, SemanticError}, LogicalPlan._, SemanticError._

import scalaz._, NonEmptyList.nel, Scalaz._, Validation.{success, failure}

trait MathLib extends Library {
  private val MathRel = Type.Numeric ⨿ Type.Interval
  private val MathAbs = Type.Numeric ⨿ Type.Interval ⨿ Type.Temporal

  object Zero {
    def apply() = Data.Int(0)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v == 0 => true
      case _                        => false
    }
  }
  object One {
    def apply() = Data.Int(1)
    def unapply(obj: Data): Boolean = obj match {
      case Data.Number(v) if v == 1 => true
      case _                        => false
    }
  }

  object ZeroF {
    def apply() = ConstantF(Zero())
    def unapply[A](obj: LogicalPlan[A]): Boolean = obj match {
      case ConstantF(Zero()) => true
      case _                 => false
    }
  }
  object OneF {
    def apply() = ConstantF(One())
    def unapply[A](obj: LogicalPlan[A]): Boolean = obj match {
      case ConstantF(One()) => true
      case _                => false
    }
  }

  object TZero {
    def apply() = Type.Const(Zero())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(Zero()) => true
      case _                  => false
    }
  }
  object TOne {
    def apply() = Type.Const(One())
    def unapply(obj: Type): Boolean = obj match {
      case Type.Const(One()) => true
      case _                 => false
    }
  }

  private val biReflexiveUnapply: Func.Untyper = partialUntyperV {
    case Type.Const(d) => success(d.dataType :: d.dataType :: Nil)
    case t             => success(t          :: t          :: Nil)
  }

  /**
   * Adds two numeric values, promoting to decimal if either operand is decimal.
   */
  val Add = Mapping("(+)", "Adds two numeric or temporal values",
    MathAbs, MathAbs :: MathRel :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: FunctorT](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(x, ZeroF())) => x.some
          case IsInvoke(_, List(ZeroF(), x)) => x.some
          case _                             => None
        }
    },
    (partialTyper {
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 + v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 + v2))

      case Type.Const(Data.Timestamp(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Timestamp(v1.plus(v2)))
      case Type.Timestamp :: Type.Interval :: Nil => Type.Timestamp
      case Type.Const(Data.Timestamp(_)) :: t2 :: Nil if t2 contains Type.Interval => Type.Timestamp
    }) ||| numericWidening,
    untyper(t => Type.typecheck(Type.Timestamp ⨿ Type.Interval, t).fold(
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
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: FunctorT](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(x, OneF())) => x.some
          case IsInvoke(_, List(OneF(), x)) => x.some
          case _                            => None
        }
    },
    (partialTyper {
      case TZero() :: _ :: Nil => TZero()
      case _ :: TZero() :: Nil => TZero()
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Int(v1 * v2))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => Type.Const(Data.Dec(v1 * v2))

      // TODO: handle interval multiplied by Dec (not provided by threeten). See SD-582.
      case Type.Const(Data.Interval(v1)) :: Type.Const(Data.Int(v2)) :: Nil => Type.Const(Data.Interval(v1.multipliedBy(v2.longValue)))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Interval(v2)) :: Nil => Type.Const(Data.Interval(v2.multipliedBy(v1.longValue)))
      case Type.Const(Data.Interval(v1)) :: t :: Nil if t contains Type.Int => Type.Interval
    }) ||| numericWidening,
    biReflexiveUnapply)

  val Power = Mapping("(^)", "Raises the first argument to the power of the second",
    Type.Numeric, Type.Numeric :: Type.Numeric :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: FunctorT](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(x, OneF())) => x.some
          case _                            => None
        }
    },
    (partialTyper {
      case _      :: TZero() :: Nil => TOne()
      case v1     :: TOne()  :: Nil => v1
      case TZero() :: _      :: Nil => TZero()
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil
          if v2.isValidInt =>
        Type.Const(Data.Int(v1.pow(v2.toInt)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Int(v2)) :: Nil
          if v2.isValidInt =>
        Type.Const(Data.Dec(v1.pow(v2.toInt)))
    }) ||| numericWidening,
    biReflexiveUnapply)

  /** Subtracts one value from another, promoting to decimal if either operand
    * is decimal.
    */
  val Subtract = Mapping("(-)", "Subtracts two numeric or temporal values",
    MathAbs, MathAbs :: MathAbs :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: FunctorT](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(x, ZeroF())) => x.some
          case InvokeF(_, List(c, x)) => c.project match {
            case ZeroF() => Negate(x).some
            case _       => None
          }
          case _ => None
        }
    },
    (partialTyper {
      case v1 :: TZero() :: Nil => v1
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
        κ(success(List(Type.Temporal ⨿ Type.Interval, Type.Temporal ⨿ Type.Interval))))),
      κ(success(List(t, Type.Interval))))))

  /**
   * Divides one value by another, promoting to decimal if either operand is decimal.
   */
  val Divide = Mapping("(/)", "Divides one numeric or interval value by another (non-zero) numeric value",
    MathRel, MathAbs :: MathRel :: Nil,
    new Func.Simplifier {
      def apply[T[_[_]]: Recursive: FunctorT](orig: LogicalPlan[T[LogicalPlan]]) =
        orig match {
          case IsInvoke(_, List(x, OneF())) => x.some
          case _                            => None
        }
    },
    (partialTyperV {
      case v1 :: TOne()  :: Nil => success(v1)
      case v1 :: TZero() :: Nil => failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil => success(Type.Const(Data.Int(v1 / v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil => success(Type.Const(Data.Dec(v1 / v2)))

      // TODO: handle interval divided by Dec (not provided by threeten). See SD-582.
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
      κ(success(List((Type.Temporal ⨿ Type.Interval), MathRel))))))

  /**
   * Aka "unary minus".
   */
  val Negate = Mapping("-", "Reverses the sign of a numeric or interval value",
    MathRel, MathRel :: Nil,
    noSimplification,
    partialTyperV {
      case Type.Const(Data.Int(v)) :: Nil      => success(Type.Const(Data.Int(-v)))
      case Type.Const(Data.Dec(v)) :: Nil      => success(Type.Const(Data.Dec(-v)))
      case Type.Const(Data.Interval(v)) :: Nil => success(Type.Const(Data.Interval(v.negated)))
      case t :: Nil if (Type.Numeric ⨿ Type.Interval) contains t => success(t)
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
      case v1 :: TOne()  :: Nil => success(v1)
      case v1 :: TZero() :: Nil =>
        failure(NonEmptyList(GenericError("Division by zero")))
      case Type.Const(Data.Int(v1)) :: Type.Const(Data.Int(v2)) :: Nil =>
        success(Type.Const(Data.Int(v1 % v2)))
      case Type.Const(Data.Number(v1)) :: Type.Const(Data.Number(v2)) :: Nil =>
        success(Type.Const(Data.Dec(v1 % v2)))
    }) ||| numericWidening,
    biReflexiveUnapply)

  def functions = Add :: Multiply :: Subtract :: Divide :: Negate :: Modulo :: Power :: Nil
}
object MathLib extends MathLib
