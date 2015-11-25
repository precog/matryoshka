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
package workflowtask

import quasar.Predef._
import quasar.{RenderTree, Terminal, NonTerminal}
import quasar.javascript._

import scalaz._, Scalaz._

/** A WorkflowTask approximately represents one request to MongoDB. */
sealed trait WorkflowTaskF[A]
object WorkflowTaskF {
  import Workflow._
  import MapReduce._

  /** A task that returns a necessarily small amount of raw data. */
  final case class PureTaskF[A](value: Bson) extends WorkflowTaskF[A]

  /** A task that merely sources data from some specified collection. */
  final case class ReadTaskF[A](value: Collection) extends WorkflowTaskF[A]

  /** A task that executes a Mongo read query. */
  final case class QueryTaskF[A](
    source: A, query: FindQuery, skip: Option[Int], limit: Option[Int])
      extends WorkflowTaskF[A]

  /** A task that executes a Mongo pipeline aggregation. */
  final case class PipelineTaskF[A](source: A, pipeline: Pipeline)
      extends WorkflowTaskF[A]

  /** A task that executes a Mongo map/reduce job. */
  final case class MapReduceTaskF[A](source: A, mapReduce: MapReduce, outAct: Option[Action])
      extends WorkflowTaskF[A]

  /** A task that executes a sequence of other tasks, one at a time, collecting
    * the results in the same collection. The first task must produce a new
    * collection, and the remaining tasks must be able to merge their results
    * into an existing collection.
    */
  final case class FoldLeftTaskF[A](head: A, tail: NonEmptyList[A])
      extends WorkflowTaskF[A]

  /** A task that evaluates some code on the server. The JavaScript function
    * must accept two parameters: the source collection, and the destination
    * collection.
    */
  // final case class EvalTaskF[A](source: A, code: Js.FuncDecl)
  //     extends WorkflowTaskF[A]

  // NB: currently unused, and hurts coverage
  // implicit def WorkflowTaskFTraverse: Traverse[WorkflowTaskF] =
  //   new Traverse[WorkflowTaskF] {
  //     def traverseImpl[G[_], A, B](fa: WorkflowTaskF[A])(f: A => G[B])(implicit G: Applicative[G]):
  //         G[WorkflowTaskF[B]] =
  //       fa match {
  //         case PureTaskF(bson) => G.point(PureTaskF[B](bson))
  //         case ReadTaskF(coll) => G.point(ReadTaskF[B](coll))
  //         case QueryTaskF(src, query, skip, limit) =>
  //           f(src).map(QueryTaskF(_, query, skip, limit))
  //         case PipelineTaskF(src, pipe) => f(src).map(PipelineTaskF(_, pipe))
  //         case MapReduceTaskF(src, mr, oa) => f(src).map(MapReduceTaskF(_, mr, oa))
  //         case FoldLeftTaskF(h, t) =>
  //           (f(h) |@| t.traverse(f))(FoldLeftTaskF(_, _))
  //       }
  //   }

  implicit def WorkflowTaskRenderTree: RenderTree ~> λ[α => RenderTree[WorkflowTaskF[α]]] =
    new (RenderTree ~> λ[α => RenderTree[WorkflowTaskF[α]]]) {
      def apply[α](ra: RenderTree[α]) = new RenderTree[WorkflowTaskF[α]] {
        val RC = RenderTree[Collection]
        val RO = RenderTree[WorkflowF[Unit]]
        val RJ = RenderTree[Js]
        val RS = RenderTree[Selector]

        val WorkflowTaskNodeType = "WorkflowTask" :: "Workflow" :: Nil

        def render(task: WorkflowTaskF[α]) = task match {
          case PureTaskF(bson) => Terminal("PureTask" :: WorkflowTaskNodeType,
            Some(bson.toString))
          case ReadTaskF(value) => RC.render(value).copy(nodeType = "ReadTask" :: WorkflowTaskNodeType)

          case QueryTaskF(source, query, skip, limit) =>
            val nt = "PipelineTask" :: WorkflowTaskNodeType
            NonTerminal(nt, (skip.shows + ", " + limit.shows).some,
              ra.render(source) ::
                Terminal("Query" :: nt, query.toString.some) ::
                Nil)

          case PipelineTaskF(source, pipeline) =>
            val nt = "PipelineTask" :: WorkflowTaskNodeType
            NonTerminal(nt, None,
              ra.render(source) ::
              NonTerminal("Pipeline" :: nt, None, pipeline.map(RO.render(_))) ::
                Nil)

          case MapReduceTaskF(source, MapReduce(map, reduce, selectorOpt, sortOpt, limitOpt, finalizerOpt, scopeOpt, jsModeOpt, verboseOpt), outAct) =>
            val nt = "MapReduceTask" :: WorkflowTaskNodeType
            NonTerminal(nt, None, List(
              ra.render(source),
              RJ.render(map),
              RJ.render(reduce),
              selectorOpt.fold(Terminal("None" :: Nil, None))(RS.render(_)),
              sortOpt.fold(Terminal("None" :: Nil, None))(keys =>
                NonTerminal("Sort" :: nt, None, keys.list map { case (expr, ot) =>
                  Terminal("Key" :: "Sort" :: nt, Some(expr.toString + " -> " + ot))
                })),
              Terminal("Limit" :: nt, Some(limitOpt.toString)),
              finalizerOpt.fold(Terminal("None" :: Nil, None))(RJ.render(_)),
              Terminal("Scope" :: nt, Some(scopeOpt.toString)),
              Terminal("JsMode" :: nt, Some(jsModeOpt.toString))
            ) ::: outAct.map(act => Terminal("Out" :: nt, Some(Action.bsonFieldName(act)))).toList)

          case FoldLeftTaskF(head, tail) =>
            NonTerminal("FoldLeftTask" :: WorkflowTaskNodeType, None, ra.render(head) :: tail.toList.map(ra.render))
        }
      }
    }
}

object PureTaskF {
  def apply[A](bson: Bson): WorkflowTaskF[A] = WorkflowTaskF.PureTaskF[A](bson)
  def unapply[A](obj: WorkflowTaskF[A]): Option[Bson] = obj match {
    case WorkflowTaskF.PureTaskF(bson) => Some(bson)
    case _                             => None
  }
}

object ReadTaskF {
  def apply[A](coll: Collection): WorkflowTaskF[A] =
    WorkflowTaskF.ReadTaskF[A](coll)
  def unapply[A](obj: WorkflowTaskF[A]): Option[Collection] = obj match {
    case WorkflowTaskF.ReadTaskF(coll) => Some(coll)
    case _                             => None
  }
}

object QueryTaskF {
  def apply[A](
    source: A, query: FindQuery, skip: Option[Int], limit: Option[Int]):
      WorkflowTaskF[A] =
    WorkflowTaskF.QueryTaskF[A](source, query, skip, limit)
  def unapply[A](obj: WorkflowTaskF[A]):
      Option[(A, FindQuery, Option[Int], Option[Int])] =
    obj match {
      case WorkflowTaskF.QueryTaskF(source, query, skip, limit) =>
        Some((source, query, skip, limit))
      case _ => None
    }
}

object PipelineTaskF {
  def apply[A](source: A, pipeline: Pipeline): WorkflowTaskF[A] =
    WorkflowTaskF.PipelineTaskF[A](source, pipeline)
  def unapply[A](obj: WorkflowTaskF[A]): Option[(A, Pipeline)] = obj match {
    case WorkflowTaskF.PipelineTaskF(source, pipeline) =>
      Some((source, pipeline))
    case _ => None
  }
}

object MapReduceTaskF {
  import MapReduce._
  def apply[A](source: A, mapReduce: MapReduce, outAct: Option[Action]): WorkflowTaskF[A] =
    WorkflowTaskF.MapReduceTaskF[A](source, mapReduce, outAct)
  def unapply[A](obj: WorkflowTaskF[A]): Option[(A, MapReduce, Option[Action])] = obj match {
    case WorkflowTaskF.MapReduceTaskF(source, mapReduce, outAct) =>
      Some((source, mapReduce, outAct))
    case _ => None
  }
}

object FoldLeftTaskF {
  def apply[A](head: A, tail: NonEmptyList[A]): WorkflowTaskF[A] =
    WorkflowTaskF.FoldLeftTaskF[A](head, tail)
  def unapply[A](obj: WorkflowTaskF[A]): Option[(A, NonEmptyList[A])] =
    obj match {
      case WorkflowTaskF.FoldLeftTaskF(head, tail) => Some((head, tail))
      case _                                       => None
    }
}
