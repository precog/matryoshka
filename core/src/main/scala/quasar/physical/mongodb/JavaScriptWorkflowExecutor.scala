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

  def mapReduce(src: Collection, dstCollectionName: String, mr: MapReduce) = {
    val dst = Collection(src.databaseName, dstCollectionName)

    tell(Call(
      Select(toJsRef(src), "mapReduce"),
      List(mr.map, mr.reduce, mr.bson(dst).toJs)))
  }

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
