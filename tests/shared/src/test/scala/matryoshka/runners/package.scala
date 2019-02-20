/*
 * Copyright 2014–2018 SlamData Inc.
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

import matryoshka.data._
import matryoshka.implicits._

import org.specs2.execute._
import org.specs2.matcher._
import scalaz._

package object runners {
  import MatchResultCombinators._

  def testRec[F[_], A](t: Fix[F], r: RecRunner[F, A])(implicit F: Functor[F])
      : MatchResult[A] = {
    r.run[Fix[F]].apply(t) and
    r.run[Mu[F]].apply(t.convertTo[Mu[F]]) and
    r.run[Nu[F]].apply(t.convertTo[Nu[F]])
  }

  def testBirec[F[_], A](t: Fix[F], r: BirecRunner[F, A])(implicit F: Functor[F])
      : MatchResult[A] = {
    r.run[Fix[F]].apply(t) and
    r.run[Mu[F]].apply(t.convertTo[Mu[F]]) and
    r.run[Nu[F]].apply(t.convertTo[Nu[F]])
  }

  def testCorec[M[_], F[_]: Functor, A]
    (a: A, r: CorecRunner[M, F, A])
    (implicit Eq0: Delay[Equal, F], S0: Delay[Show, F])
      : Result =
    r.run[Fix[F]].apply(a).toResult and
    r.run[Mu[F]].apply(a).toResult and
    r.run[Nu[F]].apply(a).toResult
}
