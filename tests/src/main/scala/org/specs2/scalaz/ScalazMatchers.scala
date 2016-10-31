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

package org.specs2.scalaz

import scalaz._

import org.specs2.matcher._

trait ScalazMatchers extends ValidationMatchers with DisjunctionMatchers { outer =>

  /** Equality matcher with a [[scalaz.Equal]] typeclass */
  def equal[T : Equal : Show](expected: T): Matcher[T] = new Matcher[T] {
    def apply[S <: T](actual: Expectable[S]): MatchResult[S] = {
      val actualT = actual.value.asInstanceOf[T]
      def test = Equal[T].equal(expected, actualT)
      def koMessage = "%s !== %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      def okMessage = "%s === %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      Matcher.result(test, okMessage, koMessage, actual)
    }
  }

  class ScalazBeHaveMatchers[T : Equal : Show](result: MatchResult[T]) {
    def equal(t: T) = result(outer.equal[T](t)(Equal[T], Show[T]))
  }

  import scala.language.implicitConversions
  implicit def scalazBeHaveMatcher[T : Equal : Show](result: MatchResult[T]) = new ScalazBeHaveMatchers(result)
}

object ScalazMatchers extends ScalazMatchers

// vim: expandtab:ts=2:sw=2
