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
import quasar._, Backend._, Errors._
import quasar.fp._
import quasar.Evaluator._

import scala.collection.JavaConverters._

import com.mongodb.client.model.{InsertOneModel, RenameCollectionOptions}
import com.mongodb.client.{MongoCollection, MongoDatabase}
import com.mongodb.{MongoClient, MongoCommandException, MongoNamespace}
import org.bson.Document
import scalaz.Scalaz._
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz.stream.io._

sealed trait MongoWrapper {
  protected def client: MongoClient

  def genTempName(col: Collection): Task[Collection] = for {
    start <- SequenceNameGenerator.startUnique
  } yield SequenceNameGenerator.Gen.generateTempName(col.databaseName).eval(start)

  private val db = Memo.mutableHashMapMemo[String, MongoDatabase] { (name: String) =>
    client.getDatabase(name)
  }

  def exists(path: Collection): Task[Boolean] = Task.delay{
    val database = client.getDatabase(path.databaseName)
    try { database.listCollectionNames().asScala.exists(name => name == path.collectionName) }
    catch { case _: Throwable => false }
  }

  // Note: this exposes the Java obj, so should be made private at some point
  def get(col: Collection): Task[MongoCollection[Document]] =
    Task.delay(db(col.databaseName).getCollection(col.collectionName))

  def rename(src: Collection, dst: Collection, semantics: RenameSemantics): Task[Unit] = {
    val drop = semantics match {
      case RenameSemantics.Overwrite => true
      case RenameSemantics.FailIfExists => false
    }

    if (src.equals(dst))
      Task.now(())
    else
      for {
        s <- get(src)
        _ <- Task.delay(s.renameCollection(
          new MongoNamespace(dst.databaseName, dst.collectionName),
          (new RenameCollectionOptions).dropTarget(drop)))
      } yield ()
  }

  def drop(col: Collection): Task[Unit] = for {
    c <- get(col)
    _ = c.drop
  } yield ()

  def dropDatabase(name: String): Task[Unit] = Task.delay(db(name).drop)

  def dropAllDatabases: Task[Unit] =
    databaseNames.flatMap(_.traverse_(dropDatabase))

  def insert(col: Collection, data: Vector[Document]): Task[Unit] = for {
    c <- get(col)
    _ = c.bulkWrite(data.map(new InsertOneModel(_)).asJava)
  } yield ()

  def databaseNames: Task[Set[String]] =
    MongoWrapper.databaseNames(client)

  def collections: Task[List[Collection]] =
    databaseNames.flatMap(_.toList.traverse(MongoWrapper.collections(_, client)))
      .map(_.join)

  def readCursor(cursor: Task[com.mongodb.client.MongoIterable[org.bson.Document]]):
      Process[ETask[ResultError, ?], Data] = {
    resource(cursor.map(_.iterator))(
      iter => Task.delay(iter.close()))(
      iter => Task.delay {
        if (iter.hasNext) {
          val obj = iter.next
          ignore(obj.remove("_id"))
          BsonCodec.toData(Bson.fromRepr(obj))
        }
        else throw Cause.End.asThrowable
      }).translate[ETask[ResultError, ?]](liftE[ResultError])
  }
}

object MongoWrapper {
  def apply(client0: com.mongodb.MongoClient) =
    new MongoWrapper { val client = client0 }

  def liftTask: (Task ~> EvaluationTask) =
    new (Task ~> EvaluationTask) {
      def apply[A](t: Task[A]) =
        EitherT(t.attempt).leftMap(e => CommandFailed(e.getMessage))
    }

  /**
  Defer an action to be performed against MongoDB, capturing exceptions
   in EvaluationError.
    */
  def delay[A](a: => A): EvaluationTask[A] =
    liftTask(Task.delay(a))

  /** List all the databases. If the client does not have sufficient permissions to list all databases, then
    * fallback to listing the authentication databases
    */
  def databaseNames(client: MongoClient): Task[Set[String]] =
    Task.delay(try {
      client.listDatabaseNames.asScala.toSet
    } catch {
      case _: MongoCommandException =>
        client.getCredentialsList.asScala.map(_.getSource).toSet
    })

  def collections(dbName: String, client: MongoClient): Task[List[Collection]] =
    Task.delay(client.getDatabase(dbName)
      .listCollectionNames.asScala.toList
      .map(Collection(dbName, _)))
}
