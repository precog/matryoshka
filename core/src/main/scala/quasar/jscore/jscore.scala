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

package quasar.jscore

import quasar.Predef._
import quasar.javascript.Js

sealed trait Operator {
  val js: String
}
abstract sealed class BinaryOperator(val js: String) extends Operator
final case object Add extends BinaryOperator("+")
final case object BitAnd extends BinaryOperator("&")
final case object BitLShift extends BinaryOperator("<<")
final case object BitNot extends BinaryOperator("~")
final case object BitOr  extends BinaryOperator("|")
final case object BitRShift  extends BinaryOperator(">>")
final case object BitXor  extends BinaryOperator("^")
final case object Lt extends BinaryOperator("<")
final case object Lte extends BinaryOperator("<=")
final case object Gt extends BinaryOperator(">")
final case object Gte extends BinaryOperator(">=")
final case object Eq extends BinaryOperator("===")
final case object Neq extends BinaryOperator("!==")
final case object Div extends BinaryOperator("/")
final case object In extends BinaryOperator("in")
final case object And extends BinaryOperator("&&")
final case object Or extends BinaryOperator("||")
final case object Mod extends BinaryOperator("%")
final case object Mult extends BinaryOperator("*")
final case object Sub extends BinaryOperator("-")
final case object Instance extends BinaryOperator("instanceof")

abstract sealed class UnaryOperator(val js: String) extends Operator
final case object Neg extends UnaryOperator("-")
final case object Not extends UnaryOperator("!")
final case object TypeOf extends UnaryOperator("typeof")

// TODO: impose JavaScript naming rules? Maybe even at compile time?
final case class Name(value: String) extends scala.AnyRef

sealed trait JsCoreF[A]
object JsCoreF {
  final case class LiteralF[A](value: Js.Lit) extends JsCoreF[A]
  final case class IdentF[A](name: Name) extends JsCoreF[A]

  final case class AccessF[A](expr: A, key: A) extends JsCoreF[A]
  final case class CallF[A](callee: A, args: List[A]) extends JsCoreF[A]
  final case class NewF[A](name: Name, args: List[A]) extends JsCoreF[A]
  final case class IfF[A](condition: A, consequent: A, alternative: A) extends JsCoreF[A]
  final case class UnOpF[A](op: UnaryOperator, arg: A) extends JsCoreF[A]
  final case class BinOpF[A](op: BinaryOperator, left: A, right: A) extends JsCoreF[A]

  final case class ArrF[A](values: List[A]) extends JsCoreF[A]
  final case class FunF[A](params: List[Name], body: A) extends JsCoreF[A]

  // NB: at runtime, JS may not preserve the order of fields, but using
  // ListMap here lets us be explicit about what result we'd like to see.
  final case class ObjF[A](values: ListMap[Name, A]) extends JsCoreF[A]

  final case class LetF[A](name: Name, expr: A, body: A) extends JsCoreF[A]

  final case class SpliceObjectsF[A](srcs: List[A]) extends JsCoreF[A]
  final case class SpliceArraysF[A](srcs: List[A]) extends JsCoreF[A]
}

object LiteralF {
  def apply[A](value: Js.Lit): JsCoreF[A] = JsCoreF.LiteralF[A](value)
  def unapply[A](obj: JsCoreF[A]): Option[Js.Lit] = obj match {
    case JsCoreF.LiteralF(value) => Some(value)
    case _ => None
  }
}

object IdentF {
  def apply[A](value: Name): JsCoreF[A] = JsCoreF.IdentF[A](value)
  def unapply[A](obj: JsCoreF[A]): Option[Name] = obj match {
    case JsCoreF.IdentF(value) => Some(value)
    case _ => None
  }
}

object AccessF {
  def apply[A](expr: A, key: A): JsCoreF[A] = JsCoreF.AccessF[A](expr, key)
  def unapply[A](obj: JsCoreF[A]): Option[(A, A)] = obj match {
    case JsCoreF.AccessF(expr, key) => Some((expr, key))
    case _ => None
  }
}

object CallF {
  def apply[A](callee: A, args: List[A]): JsCoreF[A] = JsCoreF.CallF[A](callee, args)
  def unapply[A](obj: JsCoreF[A]): Option[(A, List[A])] = obj match {
    case JsCoreF.CallF(callee, args) => Some((callee, args))
    case _ => None
  }
}

object NewF {
  def apply[A](name: Name, args: List[A]): JsCoreF[A] = JsCoreF.NewF[A](name, args)
  def unapply[A](obj: JsCoreF[A]): Option[(Name, List[A])] = obj match {
    case JsCoreF.NewF(name, args) => Some((name, args))
    case _ => None
  }
}

object IfF {
  def apply[A](condition: A, consequent: A, alternative: A): JsCoreF[A] = JsCoreF.IfF[A](condition, consequent, alternative)
  def unapply[A](obj: JsCoreF[A]): Option[(A, A, A)] = obj match {
    case JsCoreF.IfF(condition, consequent, alternative) => Some((condition, consequent, alternative))
    case _ => None
  }
}

object UnOpF {
  def apply[A](op: UnaryOperator, arg: A): JsCoreF[A] = JsCoreF.UnOpF[A](op, arg)
  def unapply[A](obj: JsCoreF[A]): Option[(UnaryOperator, A)] = obj match {
    case JsCoreF.UnOpF(op, arg) => Some((op, arg))
    case _ => None
  }
}

object BinOpF {
  def apply[A](op: BinaryOperator, left: A, right: A): JsCoreF[A] = JsCoreF.BinOpF[A](op, left, right)
  def unapply[A](obj: JsCoreF[A]): Option[(BinaryOperator, A, A)] = obj match {
    case JsCoreF.BinOpF(op, left, right) => Some((op, left, right))
    case _ => None
  }
}

object ArrF {
  def apply[A](values: List[A]): JsCoreF[A] = JsCoreF.ArrF[A](values)
  def unapply[A](obj: JsCoreF[A]): Option[List[A]] = obj match {
    case JsCoreF.ArrF(values) => Some(values)
    case _ => None
  }
}

object FunF {
  def apply[A](params: List[Name], body: A): JsCoreF[A] = JsCoreF.FunF[A](params, body)
  def unapply[A](obj: JsCoreF[A]): Option[(List[Name], A)] = obj match {
    case JsCoreF.FunF(params, body) => Some((params, body))
    case _ => None
  }
}

object ObjF {
  def apply[A](values: ListMap[Name, A]): JsCoreF[A] = JsCoreF.ObjF[A](values)
  def unapply[A](obj: JsCoreF[A]): Option[ListMap[Name, A]] = obj match {
    case JsCoreF.ObjF(values) => Some(values)
    case _ => None
  }
}

object LetF {
  def apply[A](name: Name, expr: A, body: A): JsCoreF[A] = JsCoreF.LetF[A](name, expr, body)
  def unapply[A](obj: JsCoreF[A]): Option[(Name, A, A)] = obj match {
    case JsCoreF.LetF(name, expr, body) => Some((name, expr, body))
    case _ => None
  }
}

object SpliceObjectsF {
  def apply[A](srcs: List[A]): JsCoreF[A] = JsCoreF.SpliceObjectsF[A](srcs)
  def unapply[A](obj: JsCoreF[A]): Option[List[A]] = obj match {
    case JsCoreF.SpliceObjectsF(srcs) => Some(srcs)
    case _ => None
  }
}

object SpliceArraysF {
  def apply[A](srcs: List[A]): JsCoreF[A] = JsCoreF.SpliceArraysF[A](srcs)
  def unapply[A](obj: JsCoreF[A]): Option[List[A]] = obj match {
    case JsCoreF.SpliceArraysF(srcs) => Some(srcs)
    case _ => None
  }
}
