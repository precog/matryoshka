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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.RenderTree
import quasar.recursionschemes.Recursive.ops._
import quasar.physical.mongodb.expression._

import scalaz._, Scalaz._

package object accumulator {

  type Accumulator = AccumOp[Expression]

  def rewriteGroupRefs(t: Accumulator)(applyVar: PartialFunction[DocVar, DocVar]) =
    t.map(rewriteExprRefs(_)(applyVar))

  val groupBsonƒ: AccumOp[Bson] => Bson = {
    case $addToSet(value) => bsonDoc("$addToSet", value)
    case $push(value)     => bsonDoc("$push", value)
    case $first(value)    => bsonDoc("$first", value)
    case $last(value)     => bsonDoc("$last", value)
    case $max(value)      => bsonDoc("$max", value)
    case $min(value)      => bsonDoc("$min", value)
    case $avg(value)      => bsonDoc("$avg", value)
    case $sum(value)      => bsonDoc("$sum", value)
  }

  def groupBson(g: Accumulator) = groupBsonƒ(g.map(_.cata(bsonƒ)))

  implicit val AccumOpTraverse = new Traverse[AccumOp] {
    def traverseImpl[G[_], A, B](fa: AccumOp[A])(f: A => G[B])(implicit G: Applicative[G]):
        G[AccumOp[B]] =
      fa match {
        case $addToSet(value) => G.map(f(value))($addToSet(_))
        case $avg(value)      => G.map(f(value))($avg(_))
        case $first(value)    => G.map(f(value))($first(_))
        case $last(value)     => G.map(f(value))($last(_))
        case $max(value)      => G.map(f(value))($max(_))
        case $min(value)      => G.map(f(value))($min(_))
        case $push(value)     => G.map(f(value))($push(_))
        case $sum(value)      => G.map(f(value))($sum(_))
      }
  }

  implicit val AccumOpRenderTree =
    RenderTree.fromToString[Accumulator]("AccumOp")
}
