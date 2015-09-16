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

import quasar.Predef._
import quasar.fp._
import quasar.recursionschemes._, Recursive.ops._
import quasar.SemanticError._
import quasar.analysis._
import quasar.sql._

import scalaz.{Tree => _, _}, Scalaz._

final case class Variables(value: Map[VarName, VarValue])
final case class VarName(value: String) {
  override def toString = ":" + value
}
final case class VarValue(value: String)

object Variables {
  def fromMap(value: Map[String, String]): Variables = Variables(value.map(t => VarName(t._1) -> VarValue(t._2)))

  def substVars[A](tree: AnnotatedTree[Expr, A], vars: Variables): SemanticError \/ AnnotatedTree[Expr, A] = {
    type S = List[(Expr, A)]
    type EitherM[A] = EitherT[Free.Trampoline, SemanticError, A]
    type M[A] = StateT[EitherM, S, A]

    def unchanged[A](t: (A, A)): M[A] =
      StateT[EitherM, S, A] { state =>
        EitherT.right(((state, t._2)).point[Free.Trampoline])
    }

    def changed(old: Expr, new0: SemanticError \/ Expr): M[Expr] =
      StateT[EitherM, S, Expr] { state =>
        EitherT(new0.map { new0 =>
          val ann = tree.attr(old)

          (((new0 -> ann) :: state, new0))
        }.point[Free.Trampoline])
    }

    exprMapUpM0[M](
      tree.root)(
      unchanged,
        unchanged,
        {
          case (old, v @ Vari(name)) if vars.value.contains(VarName(name)) =>
            val varValue = vars.value(VarName(name))
            val parsed = (new SQLParser()).parseExpr(varValue.value)
              .leftMap(err => VariableParseError(VarName(name), varValue, err))
            changed(old, parsed)
          case t => changed(t._1, \/-(t._2))
        },
        unchanged,
        unchanged,
        unchanged
    ).run(Nil).run.run.map {
      case (tuples, root) =>
        val map1 = tuples.foldLeft(new java.util.IdentityHashMap[Expr, A]) { // TODO: Use ordinary map when AnnotatedTree has been off'd
          case (map, (k, v)) => ignore(map.put(k, v)); map
        }

        Tree[Expr](root, _.children).annotate(map1.get(_))
    }
  }
}
