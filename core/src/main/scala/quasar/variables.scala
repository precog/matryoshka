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
import quasar.recursionschemes._
import quasar.SemanticError._
import quasar.sql._

import scalaz._, Scalaz._

final case class Variables(value: Map[VarName, VarValue])
final case class VarName(value: String) {
  override def toString = ":" + value
}
final case class VarValue(value: String)

object Variables {
  def fromMap(value: Map[String, String]): Variables = Variables(value.map(t => VarName(t._1) -> VarValue(t._2)))

  def substVarsƒ[A](vars: Variables): ExprF[Expr] => SemanticError \/ Expr = {
    case VariF(name) =>
      vars.value.get(VarName(name)).fold[SemanticError \/ Expr](
        UnboundVariable(VarName(name)).left)(
        varValue => (new SQLParser()).parseExpr(varValue.value)
          .leftMap(VariableParseError(VarName(name), varValue, _)))
    case x => Fix(x).right
  }
}
