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
import quasar.fp._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._

import scalaz._, Scalaz._

private[mongodb] object execution {
  import Workflow._

  final case class Count(
    query:      Option[Selector],
    skip:       Option[Long],
    limit:      Option[Long])

  final case class Distinct(
    field:      BsonField.Name,
    query:      Option[Selector])

  final case class Find(
    query:      Option[Selector],
    projection: Option[Bson.Doc],
    sort:       Option[NonEmptyList[(BsonField, SortType)]],
    skip:       Option[Long],
    limit:      Option[Long])

  /** Extractor to determine whether a `$Group` represents a simple `count()`.
    */
  object Countable {
    def unapply(op: PipelineOp): Option[BsonField.Name] = op match {
      case $Group((), Grouped(map), \/-($literal(Bson.Null))) if map.size ≟ 1 =>
        map.headOption
          .filter(_._2 == $sum($literal(Bson.Int32(1))))
          .map(_._1)
      case _ => None
    }
  }

  object Distinctable {
    def unapply(pipeline: workflowtask.Pipeline): Option[(BsonField.Name, BsonField.Name)] =
      pipeline match {
        case List($Group((), Grouped(map), by), $Project((), Reshape(fields), IdHandling.IgnoreId | IdHandling.ExcludeId))
            if map.isEmpty && fields.size ≟ 1 =>
          fields.headOption.fold[Option[(BsonField.Name, BsonField.Name)]] (None)(field =>
            (by, field) match {
              case (\/-($var(DocField(origField @ BsonField.Name(_)))), (newField, \/-($var(DocField(IdName))))) =>
                (origField, newField).some
              case (-\/(Reshape(map)), (newField, \/-($var(DocField(BsonField.Path(NonEmptyList(IdName, x))))))) if map.size ≟ 1 =>
                map.get(x).flatMap {
                  case \/-($var(DocField(origField @ BsonField.Name(_)))) => (origField, newField).some
                  case _ => None
                }
              case _ => None
            })
        case _ => None
      }
  }

  object Projectable {
    def unapply(op: PipelineOp): Option[Bson.Doc] = op match {
      case proj @ $Project((), Reshape(map), _)
          if map.all(_ == \/-($include())) =>
        proj.rhs.some
      case _ => None
    }
  }

  def extractRange(pipeline: workflowtask.Pipeline):
      ((workflowtask.Pipeline, workflowtask.Pipeline),
        (Option[Long], Option[Long])) =
    pipeline match {
      case Nil                                => ((Nil, Nil), (None,   None))
      case $Limit((), l) :: $Skip((), s) :: t => ((Nil, t),   (s.some, (l - s).some))
      case $Limit((), l)                 :: t => ((Nil, t),   (None,   l.some))
      case                  $Skip((), s) :: t => ((Nil, t),   (s.some, None))
      case h                             :: t => (h :: (_: workflowtask.Pipeline)).first.first(extractRange(t))
    }
}
