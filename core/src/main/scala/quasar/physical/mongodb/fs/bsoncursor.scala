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

package quasar.physical.mongodb.fs

import quasar.Predef.{Boolean, Vector}
import quasar.Data
import quasar.fs.DataCursor
import quasar.physical.mongodb._

import scala.Option
import scala.collection.JavaConverters._

import org.bson.BsonDocument
import scalaz._, Id._
import scalaz.concurrent.Task
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.std.vector._

object bsoncursor {
  implicit val bsonCursorDataCursor: DataCursor[MongoDbIO, BsonCursor] =
    new DataCursor[MongoDbIO, BsonCursor] {
      def nextChunk(cursor: BsonCursor) = {
        // NB: `null` is used as a sentinel value to indicate input is
        //     exhausted, because Java.
        def nextChunk0 =
          MongoDbIO.async(cursor.next) map (r =>
            Option(r).map(_.asScala.toVector).orZero.map(toData))

        isClosed(cursor) ifM (Vector[Data]().point[MongoDbIO], nextChunk0)
      }

      def close(cursor: BsonCursor) =
        MongoDbIO.liftTask(Task.delay(cursor.close()))

      ////

      val withoutId: BsonDocument => BsonDocument =
        d => (d: Id[BsonDocument]) map (_ remove "_id") as d

      val toData: BsonDocument => Data =
        (BsonCodec.toData _) compose Bson.fromRepr compose withoutId

      def isClosed(cursor: BsonCursor): MongoDbIO[Boolean] =
        MongoDbIO.liftTask(Task.delay(cursor.isClosed))
    }
}
