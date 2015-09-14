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

package quasar.physical.mongodb.expression

import quasar.Predef._
import quasar.physical.mongodb.Bson

sealed trait ExprOp[A]
object ExprOp {
  final case class $includeF[A]() extends ExprOp[A]
  final case class $varF[A](docVar: DocVar) extends ExprOp[A]

  final case class $andF[A](first: A, second: A, others: A*) extends ExprOp[A]
  final case class $orF[A](first: A, second: A, others: A*)  extends ExprOp[A]
  final case class $notF[A](value: A)                        extends ExprOp[A]

  final case class $setEqualsF[A](left: A, right: A) extends ExprOp[A]
  final case class $setIntersectionF[A](left: A, right: A) extends ExprOp[A]
  final case class $setDifferenceF[A](left: A, right: A) extends ExprOp[A]
  final case class $setUnionF[A](left: A, right: A) extends ExprOp[A]
  final case class $setIsSubsetF[A](left: A, right: A) extends ExprOp[A]

  final case class $anyElementTrueF[A](value: A) extends ExprOp[A]
  final case class $allElementsTrueF[A](value: A) extends ExprOp[A]

  final case class $cmpF[A](left: A, right: A) extends ExprOp[A]
  final case class $eqF[A](left: A, right: A)  extends ExprOp[A]
  final case class $gtF[A](left: A, right: A)  extends ExprOp[A]
  final case class $gteF[A](left: A, right: A) extends ExprOp[A]
  final case class $ltF[A](left: A, right: A)  extends ExprOp[A]
  final case class $lteF[A](left: A, right: A) extends ExprOp[A]
  final case class $neqF[A](left: A, right: A) extends ExprOp[A]

  final case class $addF[A](left: A, right: A) extends ExprOp[A]
  final case class $divideF[A](left: A, right: A) extends ExprOp[A]
  final case class $modF[A](left: A, right: A) extends ExprOp[A]
  final case class $multiplyF[A](left: A, right: A) extends ExprOp[A]
  final case class $subtractF[A](left: A, right: A) extends ExprOp[A]

  final case class $concatF[A](first: A, second: A, others: A*)
      extends ExprOp[A]
  final case class $strcasecmpF[A](left: A, right: A) extends ExprOp[A]
  final case class $substrF[A](value: A, start: A, count: A) extends ExprOp[A]
  final case class $toLowerF[A](value: A) extends ExprOp[A]
  final case class $toUpperF[A](value: A) extends ExprOp[A]

  final case class $metaF[A]() extends ExprOp[A]

  final case class $sizeF[A](array: A) extends ExprOp[A]

  final case class $arrayMapF[A](input: A, as: DocVar.Name, in: A)
      extends ExprOp[A]
  final case class $letF[A](vars: ListMap[DocVar.Name, A], in: A)
      extends ExprOp[A]
  final case class $literalF[A](value: Bson) extends ExprOp[A]

  final case class $dayOfYearF[A](date: A)   extends ExprOp[A]
  final case class $dayOfMonthF[A](date: A)  extends ExprOp[A]
  final case class $dayOfWeekF[A](date: A)   extends ExprOp[A]
  final case class $yearF[A](date: A)        extends ExprOp[A]
  final case class $monthF[A](date: A)       extends ExprOp[A]
  final case class $weekF[A](date: A)        extends ExprOp[A]
  final case class $hourF[A](date: A)        extends ExprOp[A]
  final case class $minuteF[A](date: A)      extends ExprOp[A]
  final case class $secondF[A](date: A)      extends ExprOp[A]
  final case class $millisecondF[A](date: A) extends ExprOp[A]

  final case class $condF[A](predicate: A, ifTrue: A, ifFalse: A)
      extends ExprOp[A]
  final case class $ifNullF[A](expr: A, replacement: A) extends ExprOp[A]
}

object $includeF {
  def apply[A](): ExprOp[A] = ExprOp.$includeF[A]()
  def unapply[A](obj: ExprOp[A]): Boolean = obj match {
    case ExprOp.$includeF() => true
    case _                 => false
  }
}
object $varF {
  def apply[A](docVar: DocVar): ExprOp[A] = ExprOp.$varF[A](docVar)
  def unapply[A](obj: ExprOp[A]): Option[DocVar] = obj match {
    case ExprOp.$varF(docVar) => Some(docVar)
    case _                   => None
  }
}

object $andF {
  def apply[A](first: A, second: A, others: A*): ExprOp[A] =
    ExprOp.$andF[A](first, second, others: _*)
  def unapplySeq[A](obj: ExprOp[A]): Option[(A, A, Seq[A])] = obj match {
    case ExprOp.$andF(first, second, others @ _*) => Some((first, second, others.toList))
    case _                                       => None
  }
}
object $orF {
  def apply[A](first: A, second: A, others: A*): ExprOp[A] =
    ExprOp.$orF[A](first, second, others: _*)
  def unapplySeq[A](obj: ExprOp[A]): Option[(A, A, Seq[A])] = obj match {
    case ExprOp.$orF(first, second, others @ _*) => Some((first, second, others.toList))
    case _                                      => None
  }
}

object $notF {
  def apply[A](value: A): ExprOp[A] = ExprOp.$notF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$notF(value) => Some(value)
    case _                  => None
  }
}

object $setEqualsF {
  def apply[A](left: A, right: A): ExprOp[A] =
    ExprOp.$setEqualsF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$setEqualsF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $setIntersectionF {
  def apply[A](left: A, right: A): ExprOp[A] =
    ExprOp.$setIntersectionF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$setIntersectionF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $setDifferenceF {
  def apply[A](left: A, right: A): ExprOp[A] =
    ExprOp.$setDifferenceF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$setDifferenceF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $setUnionF {
  def apply[A](left: A, right: A): ExprOp[A] =
    ExprOp.$setUnionF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$setUnionF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $setIsSubsetF {
  def apply[A](left: A, right: A): ExprOp[A] =
    ExprOp.$setIsSubsetF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$setIsSubsetF(left, right) => Some((left, right))
    case _                              => None
  }
}

object $anyElementTrueF {
  def apply[A](value: A): ExprOp[A] = ExprOp.$anyElementTrueF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$anyElementTrueF(value) => Some(value)
    case _                             => None
  }
}
object $allElementsTrueF {
  def apply[A](value: A): ExprOp[A] = ExprOp.$allElementsTrueF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$anyElementTrueF(value) => Some(value)
    case _                             => None
  }
}

object $cmpF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$cmpF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$cmpF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $eqF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$eqF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$eqF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $gtF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$gtF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$gtF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $gteF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$gteF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$gteF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $ltF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$ltF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$ltF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $lteF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$lteF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$lteF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $neqF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$neqF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$neqF(left, right) => Some((left, right))
    case _                              => None
  }
}

object $addF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$addF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$addF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $divideF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$divideF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$divideF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $modF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$modF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$modF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $multiplyF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$multiplyF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$multiplyF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $subtractF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$subtractF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$subtractF(left, right) => Some((left, right))
    case _                              => None
  }
}

object $concatF {
  def apply[A](first: A, second: A, others: A*): ExprOp[A] = ExprOp.$concatF[A](first, second, others: _*)
  def unapplySeq[A](obj: ExprOp[A]): Option[(A, A, Seq[A])] = obj match {
    case ExprOp.$concatF(first, second, others @ _*) => Some((first, second, others.toList))
    case _                              => None
  }
}
object $strcasecmpF {
  def apply[A](left: A, right: A): ExprOp[A] = ExprOp.$strcasecmpF[A](left, right)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$strcasecmpF(left, right) => Some((left, right))
    case _                              => None
  }
}
object $substrF {
  def apply[A](value: A, start: A, count: A): ExprOp[A] = ExprOp.$substrF[A](value, start, count)
  def unapply[A](obj: ExprOp[A]): Option[(A, A, A)] = obj match {
    case ExprOp.$substrF(value, start, count) => Some((value, start, count))
    case _                                   => None
  }
}
object $toLowerF {
  def apply[A](value: A): ExprOp[A] = ExprOp.$toLowerF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$toLowerF(value) => Some(value)
    case _          => None
  }
}
object $toUpperF {
  def apply[A](value: A): ExprOp[A] = ExprOp.$toUpperF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$toUpperF(value) => Some(value)
    case _          => None
  }
}

object $metaF {
  def apply[A](): ExprOp[A] = ExprOp.$metaF[A]()
  def unapply[A](obj: ExprOp[A]): Boolean = obj match {
    case ExprOp.$metaF() => true
    case _      => false
  }
}

object $sizeF {
  def apply[A](array: A): ExprOp[A] = ExprOp.$sizeF[A](array)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$sizeF(array) => Some(array)
    case _          => None
  }
}

object $arrayMapF {
  def apply[A](input: A, as: DocVar.Name, in: A): ExprOp[A] =
    ExprOp.$arrayMapF[A](input, as, in)
  def unapply[A](obj: ExprOp[A]): Option[(A, DocVar.Name, A)] = obj match {
    case ExprOp.$arrayMapF(input, as, in) => Some((input, as, in))
    case _                       => None
  }
}
object $letF {
  def apply[A](vars: ListMap[DocVar.Name, A], in: A): ExprOp[A] =
    ExprOp.$letF[A](vars, in)
  def unapply[A](obj: ExprOp[A]): Option[(ListMap[DocVar.Name, A], A)] =
    obj match {
      case ExprOp.$letF(vars, in) => Some((vars, in))
      case _             => None
    }
}
object $literalF {
  def apply[A](value: Bson): ExprOp[A] = ExprOp.$literalF[A](value)
  def unapply[A](obj: ExprOp[A]): Option[Bson] = obj match {
    case ExprOp.$literalF(value) => Some(value)
    case _              => None
  }
}

object $dayOfYearF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$dayOfYearF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$dayOfYearF(date) => Some(date)
    case _          => None
  }
}
object $dayOfMonthF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$dayOfMonthF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$dayOfMonthF(date) => Some(date)
    case _          => None
  }
}
object $dayOfWeekF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$dayOfWeekF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$dayOfWeekF(date) => Some(date)
    case _          => None
  }
}
object $yearF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$yearF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$yearF(date) => Some(date)
    case _          => None
  }
}
object $monthF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$monthF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$monthF(date) => Some(date)
    case _          => None
  }
}
object $weekF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$weekF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$weekF(date) => Some(date)
    case _          => None
  }
}
object $hourF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$hourF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$hourF(date) => Some(date)
    case _          => None
  }
}
object $minuteF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$minuteF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$minuteF(date) => Some(date)
    case _          => None
  }
}
object $secondF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$secondF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$secondF(date) => Some(date)
    case _          => None
  }
}
object $millisecondF {
  def apply[A](date: A): ExprOp[A] = ExprOp.$millisecondF[A](date)
  def unapply[A](obj: ExprOp[A]): Option[A] = obj match {
    case ExprOp.$millisecondF(date) => Some(date)
    case _          => None
  }
}

object $condF {
  def apply[A](predicate: A, ifTrue: A, ifFalse: A): ExprOp[A] = ExprOp.$condF[A](predicate, ifTrue, ifFalse)
  def unapply[A](obj: ExprOp[A]): Option[(A, A, A)] = obj match {
    case ExprOp.$condF(pred, t, f) => Some((pred, t, f))
    case _                        => None
  }
}
object $ifNullF {
  def apply[A](expr: A, replacement: A): ExprOp[A] = ExprOp.$ifNullF[A](expr, replacement)
  def unapply[A](obj: ExprOp[A]): Option[(A, A)] = obj match {
    case ExprOp.$ifNullF(expr, replacement) => Some((expr, replacement))
    case _                                 => None
  }
}
