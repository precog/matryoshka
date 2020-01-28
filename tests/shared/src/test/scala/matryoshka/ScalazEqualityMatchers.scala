/*
 * Copyright 2014–2019 SlamData Inc.
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

package matryoshka

import slamdata.Predef._

import org.specs2.matcher._
import scalaz.{\/, Equal, Show}

// cribbed from quasar
trait ScalazEqualityMatchers { self =>
    /** Equality matcher with a [[scalaz.Equal]] typeclass */
  def equal[T : Equal : Show](expected: T): Matcher[T] = new Matcher[T] {
    def apply[S <: T](actual: Expectable[S]): MatchResult[S] = {
      val actualT: T = actual.value
      def test = Equal[T].equal(expected, actualT)
      def koMessage = "%s !== %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      def okMessage = "%s === %s".format(Show[T].shows(actualT), Show[T].shows(expected))
      Matcher.result(test, okMessage, koMessage, actual)
    }
  }

  def beRightDisjunction[T](t: ValueCheck[T]) = RightDisjunctionCheckedMatcher(t)
  def beRightDisjunction[T] = RightDisjunctionMatcher[T]()

  def beLeftDisjunction[T](t: ValueCheck[T]) = LeftDisjunctionCheckedMatcher(t)
  def beLeftDisjunction[T] = LeftDisjunctionMatcher[T]()

  case class RightDisjunctionMatcher[T]() extends OptionLikeMatcher[({type l[a]= _ \/ a})#l, T, T]("\\/-", (_:Any \/ T).toEither.toOption)
  case class RightDisjunctionCheckedMatcher[T](check: ValueCheck[T]) extends OptionLikeCheckedMatcher[({type l[a]= _ \/ a})#l, T, T]("\\/-", (_:Any \/ T).toEither.toOption, check)

  case class LeftDisjunctionMatcher[T]() extends OptionLikeMatcher[({type l[a]= a \/ _})#l, T, T]("-\\/", (_:T \/ Any).toEither.left.toOption)
  case class LeftDisjunctionCheckedMatcher[T](check: ValueCheck[T]) extends OptionLikeCheckedMatcher[({type l[a]=a \/ _})#l, T, T]("-\\/", (_: T \/Any).toEither.left.toOption, check)

  implicit final class ScalazBeHaveMatchers[T : Equal : Show](result: MatchResult[T]) {
    def equal(t: T) = result.applyMatcher(self.equal[T](t))
  }
}

object ScalazEqualityMatchers extends ScalazEqualityMatchers
