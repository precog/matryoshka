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
package physical
package mongodb

import quasar.Predef._
import quasar.javascript.Js._
import quasar.physical.mongodb.workflowtask._

import scalaz._

/** Implements the necessary operations for executing a `Workflow` against
  * MongoDB.
  */
private[mongodb] final class JavaScriptWorkflowExecutor
  extends WorkflowExecutor[JavaScriptLog] {

  import JavaScriptWorkflowExecutor._
  import MapReduce.OutputCollection

  def tell(stmt: Stmt): JavaScriptLog[Unit] =
    WriterT.tell(Vector(stmt))

  def aggregate(src: Collection, pipeline: Pipeline) =
    tell(Call(
      Select(toJsRef(src), "aggregate"),
      List(
        AnonElem(pipeline map (_.bson.toJs)),
        AnonObjDecl(List("allowDiskUse" -> Bool(true))))))

  def drop(coll: Collection) =
    tell(Call(Select(toJsRef(coll), "drop"), List()))

  def insert(dst: Collection, values: List[Bson.Doc]) =
    tell(Call(
      Select(toJsRef(dst), "insert"),
      List(AnonElem(values map (_.toJs)))))

  def mapReduce(src: Collection, dst: OutputCollection, mr: MapReduce) =
    tell(Call(
      Select(toJsRef(src), "mapReduce"),
      List(mr.map, mr.reduce, mr.toCollBson(dst).toJs)))

  def rename(src: Collection, dst: Collection) =
    tell(Call(
      Select(toJsRef(src), "renameCollection"),
      List(Str(dst.collectionName), Bool(true))))
}

private[mongodb] object JavaScriptWorkflowExecutor {
  // NB: This pattern differs slightly from the similar pattern in Js,
  //     which allows leading '_'s.
  val SimpleCollectionNamePattern =
    "[a-zA-Z][_a-zA-Z0-9]*(?:\\.[a-zA-Z][_a-zA-Z0-9]*)*".r

  def toJsRef(col: Collection) = col.collectionName match {
    case SimpleCollectionNamePattern() =>
      Select(Ident("db"), col.collectionName)

    case _ =>
      Call(Select(Ident("db"), "getCollection"), List(Str(col.collectionName)))
  }
}
