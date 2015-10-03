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

package quasar

import scala.{Predef => P}
import scala.{collection => C}
import scala.collection.{immutable => I}
import scala.inline
import scala.{runtime => R}


object Predef extends LowPriorityImplicits {
  type deprecated = scala.deprecated
  type tailrec = scala.annotation.tailrec
  type SuppressWarnings = java.lang.SuppressWarnings

  type AnyVal = scala.AnyVal

  type Array[T] = scala.Array[T]
  val  Array = scala.Array
  type Boolean = scala.Boolean
  type Byte = scala.Byte
  type Char = scala.Char
  type Double = scala.Double
  val  Function = scala.Function
  type Int = scala.Int
  val  Int = scala.Int
  type Long = scala.Long
  val  Long = scala.Long
  type PartialFunction[-A, +B] = scala.PartialFunction[A, B]
  val  PartialFunction         = scala.PartialFunction
  type String = P.String
  val  StringContext = scala.StringContext
  type Symbol = scala.Symbol
  val  Symbol  = scala.Symbol
  type Unit = scala.Unit
  type Vector[+A] = scala.Vector[A]
  val  Vector     = scala.Vector

  type BigDecimal = scala.math.BigDecimal
  val  BigDecimal = scala.math.BigDecimal
  type BigInt = scala.math.BigInt
  val  BigInt = scala.math.BigInt

  type Iterable[+A] = C.Iterable[A]
  type IndexedSeq[+A] = C.IndexedSeq[A]

  type ListMap[A, +B] = I.ListMap[A, B]
  val  ListMap        = I.ListMap
  type Map[A, +B] = I.Map[A, B]
  val  Map        = I.Map
  type Set[A] = I.Set[A]
  val  Set    = I.Set
  type Seq[+A] = I.Seq[A]
  type Stream[+A] = I.Stream[A]
  val  Stream     = I.Stream
  val  #::        = Stream.#::

  // NB: not using scala.Predef.??? or scala.NotImplementedError because specs2
  // intercepts the result in a useless way.
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Throw"))
  def ??? : Nothing = throw new java.lang.RuntimeException("not implemented")

  def implicitly[T](implicit e: T) = P.implicitly[T](e)

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.ExplicitImplicitTypes"))
  implicit def $conforms[A]: P.<:<[A, A] = P.$conforms[A]
  implicit def ArrowAssoc[A]: A => P.ArrowAssoc[A] = P.ArrowAssoc[A] _
  implicit def augmentString(x: String): I.StringOps = P.augmentString(x)
  implicit def genericArrayOps[T]: Array[T] => C.mutable.ArrayOps[T] =
    P.genericArrayOps[T] _
  implicit val wrapString: String => I.WrappedString = P.wrapString _
  implicit val unwrapString: I.WrappedString => String = P.unwrapString _
  @inline implicit val booleanWrapper: Boolean => R.RichBoolean =
    P.booleanWrapper _
  @inline implicit val charWrapper: Char => R.RichChar = P.charWrapper _
  @inline implicit val intWrapper: Int => R.RichInt = P.intWrapper _

  // would rather not have these, but …
  def print(x: scala.Any)   = scala.Console.print(x)
  def println()             = scala.Console.println()
  def println(x: scala.Any) = scala.Console.println(x)

  // remove or replace these
  type List[+A] = I.List[A] // use scalaz.IList instead
  val  List     = I.List
  val  Nil      = I.Nil
  val  ::       = I.::
  type Option[A] = scala.Option[A] // use scalaz.Maybe instead
  val  Option    = scala.Option
  val  None      = scala.None
  val  Some      = scala.Some
  type Nothing = scala.Nothing // make functors invariant
  type Throwable = java.lang.Throwable
  type RuntimeException = java.lang.RuntimeException
}

abstract class LowPriorityImplicits {
  implicit def genericWrapArray[T]:
      scala.Array[T] => C.mutable.WrappedArray[T] =
    P.genericWrapArray[T] _
}
