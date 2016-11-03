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

package matryoshka.specs2.scalacheck

import scala.{Any, StringContext}
import scala.Predef.Set

import org.scalacheck._
import org.scalacheck.util.{FreqMap, Pretty}
import org.specs2.mutable.SpecificationLike
import org.specs2.scalacheck.{Parameters, ScalaCheckPropertyCheck}
import org.specs2.specification.core.Fragments

// TODO[specs2]: specs2 3.7.2 has something similar to this, so we should
//               consider adopting that when upgrading to it.
trait CheckAll extends SpecificationLike with ScalaCheckPropertyCheck {
  def checkAll(props: Properties)(implicit p: Parameters, f: FreqMap[Set[Any]] => Pretty) = {
    s"${props.name} must satisfy" >> {
      Fragments.foreach(props.properties) { case (name, prop) => Fragments(name in check(prop, p, f)) }
    }
  }
}
