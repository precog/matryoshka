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

// import quasar.Predef._
import quasar.RenderTree, RenderTree.ops._

final case class JsFn(base: Name, expr: JsCore) {
  def apply(x: JsCore) = expr.substitute(Ident(base), x)

  def >>>(that: JsFn): JsFn =
    if (this == JsFn.identity) that
    else if (that == JsFn.identity) this
    else JsFn(this.base, Let(that.base, this.expr, that.expr).simplify)

  override def toString = toUnsafeJs(apply(ident("_")).simplify).pprint(0)

  val commonBase = Name("$")
  override def equals(obj: scala.Any) = obj match {
    case that @ JsFn(_, _) => apply(Ident(commonBase)).simplify == that.apply(Ident(commonBase)).simplify
    case _ => false
  }
  override def hashCode = apply(Ident(commonBase)).simplify.hashCode
}
object JsFn {
  val base = Name("__val")

  val identity = {
    JsFn(base, Ident(base))
  }

  def const(x: JsCore) = JsFn(Name("__unused"), x)

  implicit val JsFnRenderTree = new RenderTree[JsFn] {
    def render(v: JsFn) = v(ident("_")).render
  }
}
