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
import quasar.{EnvironmentError2, EnvErr2T}
import quasar.fp.prism._
import quasar.fs.Positive
import quasar.physical.mongodb.workflowtask._

import java.lang.IllegalArgumentException

import com.mongodb._
import org.bson.Document
import scalaz._, Scalaz._

/** Implementation class for a WorkflowExecutor in the `MongoDbIO` monad. */
private[mongodb] final class MongoDbWorkflowExecutor
  extends WorkflowExecutor[MongoDbIO] {

  import MapReduce._

  def aggregate(src: Collection, pipeline: Pipeline) =
    MongoDbIO.aggregate_(src, pipeline map(_.bson), true)

  def drop(c: Collection) =
    MongoDbIO.dropCollection(c)

  def insert(dst: Collection, values: List[Bson.Doc]) =
    MongoDbIO.insert(dst, values.map(_.repr))

  def mapReduce(src: Collection, dst: OutputCollection, mr: MapReduce) =
    MongoDbIO.mapReduce_(src, dst, mr)

  def rename(src: Collection, dst: Collection) =
    MongoDbIO.rename(src, dst, RenameSemantics.Overwrite)
}

private[mongodb] object MongoDbWorkflowExecutor {
  import EnvironmentError2._

  /** The minimum MongoDbIO version required to be able to execute `Workflow`s. */
  val MinMongoDbVersion = List(2, 6, 0)

  /** Catch MongoExceptions and attempt to convert to EnvironmentError2. */
  val liftEnvErr: MongoDbIO ~> EnvErr2T[MongoDbIO, ?] =
    new (MongoDbIO ~> EnvErr2T[MongoDbIO, ?]) {
      def apply[A](m: MongoDbIO[A]) = EitherT(m.attemptMongo.run flatMap {
        case -\/(ex: MongoSocketOpenException) =>
          connectionFailed(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex: MongoSocketException) =>
          connectionFailed(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex) if ex.getMessage contains "Command failed with error 18: 'auth failed'" =>
          invalidCredentials(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex) =>
          MongoDbIO.fail(ex)

        case \/-(a) =>
          a.right.point[MongoDbIO]
      })
    }
}
