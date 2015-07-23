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

package slamdata.engine

import slamdata.Predef._

import slamdata.engine.analysis._
import slamdata.engine.fp._
import slamdata.engine.sql._
import slamdata.engine.SemanticError._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._

final case class Variables(value: Map[VarName, VarValue])
final case class VarName(value: String) {
  override def toString = ":" + value
}
final case class VarValue(value: String)

object Variables {
  def fromMap(value: Map[String, String]): Variables = Variables(value.map(t => VarName(t._1) -> VarValue(t._2)))

  def substVars[A](tree: AnnotatedTree[Node, A], vars: Variables): Error \/ AnnotatedTree[Node, A] = {
    type S = List[(Node, A)]
    type EitherM[A] = EitherT[Free.Trampoline, Error, A]
    type M[A] = StateT[EitherM, S, A]

    def unchanged[A <: Node](t: (A, A)): M[A] = changed(t._1, \/- (t._2))

    def changed[A <: Node](old: A, new0: Error \/ A): M[A] = StateT[EitherM, S, A] { state =>
      EitherT(new0.map { new0 =>
        val ann = tree.attr(old)

        (((new0 -> ann) :: state, new0))
      }.point[Free.Trampoline])
    }

    tree.root.mapUpM0[M](
      unchanged _,
      unchanged _,
      {
        case (old, v @ Vari(name)) if vars.value.contains(VarName(name)) =>
          val varValue = vars.value(VarName(name))
          val parsed = (new SQLParser()).parseExpr(varValue.value)
            .leftMap(err => VariableParseError(VarName(name), varValue, err))
          changed(old, parsed)

        case t => unchanged(t)
      },
      unchanged _,
      unchanged _,
      unchanged _
    ).run(Nil).run.run.map {
      case (tuples, root) =>
        val map1 = tuples.foldLeft(new java.util.IdentityHashMap[Node, A]) { // TODO: Use ordinary map when AnnotatedTree has been off'd
          case (map, (k, v)) => ignore(map.put(k, v)); map
        }

        Tree[Node](root, _.children).annotate(map1.get(_))
    }
  }
}
