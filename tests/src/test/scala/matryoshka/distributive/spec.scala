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

package matryoshka.distributive

import matryoshka._, Recursive.ops._
import matryoshka.exp._

import java.lang.String
import scala.{Boolean, Function, Int, None, Option, Predef, Symbol, Unit},
  Predef.{implicitly, wrapString}
import scala.collection.immutable.{List, Map, Nil, ::}

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._
import scalaz._, Scalaz._

class DistributiveLawSpec
    extends Specification with ScalaCheck with ScalazMatchers {

  "distCata" should {
    "behave like cata" in {
      val v = mul(num(0), mul(num(0), num(1)))
      val cata = v.cata(eval)
      v.gcata[Id, Int](distCata, eval) must equal(cata)
      v.convertTo[Mu].gcata[Id, Int](distCata, eval) must equal(cata)
    }
  }

  "distPara" should {
    "behave like para" in {
      val v = mul(num(0), mul(num(0), num(1)))
      val para = v.para(peval[Fix])
      v.gcata[(Fix[Exp], ?), Int](distPara, peval) must equal(para)
      v.convertTo[Mu].gcata[(Mu[Exp], ?), Int](distPara, peval) must equal(para)
    }
  }

  "distAna" should {
    "behave like ana" ! prop { (i: Int) =>
      val ana = i.ana[Fix, Exp](extractFactors)
      i.gana[Fix, Exp, Id](distAna, extractFactors) must equal(ana)
      i.gana[Mu, Exp, Id](distAna, extractFactors) must equal(ana.convertTo[Mu])
    }
  }

  "ghylo" should {
    "behave like hylo with distCata/distAna" ! prop { (i: Int) =>
      i.ghylo[Exp, Id, Id, Int](distCata, distAna, eval, extractFactors) must
      equal(i.hylo(eval, extractFactors))
    }

    "behave like chrono with distHisto/distFutu" ! prop { (i: Int) =>
      i.ghylo[Exp, Cofree[Exp, ?], Free[Exp, ?], Fix[Exp]](
        distHisto, distFutu, partialEval[Fix], extract2and3) must
      equal(i.chrono(partialEval[Fix], extract2and3))
    }
  }
}
