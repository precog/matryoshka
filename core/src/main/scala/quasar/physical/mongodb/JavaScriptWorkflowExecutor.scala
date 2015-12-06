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
import quasar.javascript.Js._
import quasar.physical.mongodb.execution._
import quasar.physical.mongodb.workflowtask._

import scalaz._
import scalaz.std.list._
import scalaz.std.vector._
import scalaz.std.option._
import scalaz.syntax.foldable._
import scalaz.syntax.functor._

/** Implements the necessary operations for executing a `Workflow` against
  * MongoDB.
  */
// https://github.com/puffnfresh/wartremover/issues/149
@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
private[mongodb] final class JavaScriptWorkflowExecutor
  extends WorkflowExecutor[JavaScriptLog, Unit] {

  import JavaScriptWorkflowExecutor._
  import MapReduce.OutputCollection
  import Workflow.$Sort

  type ExprS[A] = State[Expr, A]

  private def tell(stmt: Stmt): JavaScriptLog[Unit] =
    WriterT.tell(Vector(stmt))

  private def foldExpr[F[_]: Foldable, A](fa: F[A])(f: (A, Expr) => Expr): ExprS[Unit] =
    fa.traverse_[ExprS](a => MonadState[State, Expr].modify(f(a, _)))

  protected def aggregate(src: Collection, pipeline: Pipeline) =
    tell(Call(
      Select(toJsRef(src), "aggregate"),
      List(
        AnonElem(pipeline map (_.bson.toJs)),
        AnonObjDecl(List("allowDiskUse" -> Bool(true))))))

  protected def aggregateCursor(src: Collection, pipeline: Pipeline) =
    aggregate(src, pipeline)

  protected def count(src: Collection, cfg: Count) = {
    val count0 = List(
      foldExpr(cfg.limit)((n, js) => Call(Select(js, "limit"), List(Num(n, false)))),
      foldExpr(cfg.skip)((n, js) => Call(Select(js, "skip"), List(Num(n, false)))))
      .sequence_[ExprS, Unit]
      .exec(Call(Select(toJsRef(src), "count"), cfg.query.toList.map(_.bson.toJs)))

    tell(count0) as 0L
  }

  protected def distinct(src: Collection, cfg: Distinct, field: BsonField.Name) = {
    val distinct0 =
      foldExpr(cfg.query)((q, js) => Call(Select(js, "filter"), List(q.bson.toJs)))
        .exec(Call(Select(toJsRef(src), "distinct"), List(Str(cfg.field.asText))))

    tell(Call(
      Select(distinct0, "map"),
      List(AnonFunDecl(List("elem"), List(Return(AnonObjDecl(List(field.asText -> Ident("elem")))))))))
  }

  protected def drop(coll: Collection) =
    tell(Call(Select(toJsRef(coll), "drop"), List()))

  protected def find(src: Collection, cfg: Find) = {
    val find0 = Call(
      Select(toJsRef(src), "find"),
      cfg.query.foldRight(cfg.projection.foldRight(List[Expr]())(_.toJs :: _))(_.bson.toJs :: _))

    tell(List(
      foldExpr(cfg.sort)((keys, f) => Call(Select(f, "sort"), List($Sort.keyBson(keys).toJs))),
      foldExpr(cfg.skip)((n, f) => Call(Select(f, "skip"), List(Num(n, false)))),
      foldExpr(cfg.limit)((n, f) => Call(Select(f, "limit"), List(Num(n, false)))))
      .sequence_[ExprS, Unit]
      .exec(find0))
  }

  protected def insert(dst: Collection, values: List[Bson.Doc]) =
    tell(Call(
      Select(toJsRef(dst), "insert"),
      List(AnonElem(values map (_.toJs)))))

  protected def mapReduce(src: Collection, dst: OutputCollection, mr: MapReduce) =
    tell(Call(
      Select(toJsRef(src), "mapReduce"),
      List(mr.map, mr.reduce, mr.toCollBson(dst).toJs)))

  protected def mapReduceCursor(src: Collection, mr: MapReduce) =
    tell(Call(
      Select(toJsRef(src), "mapReduce"),
      List(mr.map, mr.reduce, mr.inlineBson.toJs)))
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
