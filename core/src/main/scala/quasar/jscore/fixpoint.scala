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
import quasar.recursionschemes.Fix

object Literal {
  def apply(value: Js.Lit): JsCore = Fix(LiteralF(value))
  def unapply(obj: JsCore): Option[Js.Lit] = LiteralF.unapply(obj.unFix)
}

object Ident {
  def apply(value: Name): JsCore = Fix(IdentF(value))
  def unapply(obj: JsCore): Option[Name] = IdentF.unapply(obj.unFix)
}

object Access {
  def apply(expr: JsCore, key: JsCore): JsCore = Fix(AccessF(expr, key))
  def unapply(obj: JsCore): Option[(JsCore, JsCore)] = AccessF.unapply(obj.unFix)
}

object Call {
  def apply(callee: JsCore, args: List[JsCore]): JsCore = Fix(CallF(callee, args))
  def unapply(obj: JsCore): Option[(JsCore, List[JsCore])] = CallF.unapply(obj.unFix)
}

object New {
  def apply(name: Name, args: List[JsCore]): JsCore = Fix(NewF(name, args))
  def unapply(obj: JsCore): Option[(Name, List[JsCore])] = NewF.unapply(obj.unFix)
}

object If {
  def apply(condition: JsCore, consequent: JsCore, alternative: JsCore): JsCore = Fix(IfF(condition, consequent, alternative))
  def unapply(obj: JsCore): Option[(JsCore, JsCore, JsCore)] = IfF.unapply(obj.unFix)
}

object UnOp {
  def apply(op: UnaryOperator, arg: JsCore): JsCore = Fix(UnOpF(op, arg))
  def unapply(obj: JsCore): Option[(UnaryOperator, JsCore)] = UnOpF.unapply(obj.unFix)
}

object BinOp {
  def apply(op: BinaryOperator, left: JsCore, right: JsCore): JsCore = Fix(BinOpF(op, left, right))
  def unapply(obj: JsCore): Option[(BinaryOperator, JsCore, JsCore)] = BinOpF.unapply(obj.unFix)
}

object Arr {
  def apply(values: List[JsCore]): JsCore = Fix(ArrF(values))
  def unapply(obj: JsCore): Option[(List[JsCore])] = ArrF.unapply(obj.unFix)
}

object Fun {
  def apply(params: List[Name], body: JsCore): JsCore = Fix(FunF(params, body))
  def unapply(obj: JsCore): Option[(List[Name], JsCore)] = FunF.unapply(obj.unFix)
}

object Obj {
  def apply(values: ListMap[Name, JsCore]): JsCore = Fix(ObjF(values))
  def unapply(obj: JsCore): Option[ListMap[Name, JsCore]] = ObjF.unapply(obj.unFix)
}

object Let {
  def apply(name: Name, expr: JsCore, body: JsCore): JsCore = Fix(LetF(name, expr, body))
  def unapply(obj: JsCore): Option[(Name, JsCore, JsCore)] = LetF.unapply(obj.unFix)
}

object SpliceObjects {
  def apply(srcs: List[JsCore]): JsCore = Fix(SpliceObjectsF(srcs))
  def unapply(obj: JsCore): Option[List[JsCore]] = SpliceObjectsF.unapply(obj.unFix)
}

object SpliceArrays {
  def apply(srcs: List[JsCore]): JsCore = Fix(SpliceArraysF(srcs))
  def unapply(obj: JsCore): Option[List[JsCore]] = SpliceArraysF.unapply(obj.unFix)
}
