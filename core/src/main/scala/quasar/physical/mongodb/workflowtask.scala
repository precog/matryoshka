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
import quasar.{RenderTree, Terminal, NonTerminal}
import quasar.fp._
import quasar.javascript._
import IdHandling._

import scalaz._, Scalaz._

/**
  A WorkflowTask approximately represents one request to MongoDB.
  */
sealed trait WorkflowTask

object WorkflowTask {
  import quasar.physical.mongodb.expression._
  import Workflow._

  type Pipeline = List[PipelineOp]

  implicit def WorkflowTaskRenderTree(implicit RC: RenderTree[Collection], RO: RenderTree[WorkflowF[Unit]], RJ: RenderTree[Js], RS: RenderTree[Selector]) =
    new RenderTree[WorkflowTask] {
      val WorkflowTaskNodeType = "WorkflowTask" :: "Workflow" :: Nil

      def render(task: WorkflowTask) = task match {
        case ReadTask(value) => RC.render(value).copy(nodeType = "ReadTask" :: WorkflowTaskNodeType)

        case PipelineTask(source, pipeline) =>
          val nt = "PipelineTask" :: WorkflowTaskNodeType
          NonTerminal(nt, None,
            render(source) ::
              NonTerminal("Pipeline" :: nt, None, pipeline.map(RO.render(_))) ::
              Nil)

        case FoldLeftTask(head, tail) =>
          NonTerminal("FoldLeftTask" :: WorkflowTaskNodeType, None,
            render(head) ::
              tail.map(render(_)).toList)

        case MapReduceTask(source, MapReduce(map, reduce, outOpt, selectorOpt, sortOpt, limitOpt, finalizerOpt, scopeOpt, jsModeOpt, verboseOpt)) =>
          val nt = "MapReduceTask" :: WorkflowTaskNodeType
          NonTerminal(nt, None,
            render(source) ::
              RJ.render(map) ::
              RJ.render(reduce) ::
              Terminal("Out" :: nt, Some(outOpt.toString)) ::
              selectorOpt.map(RS.render(_)).getOrElse(Terminal("None" :: Nil, None)) ::
              sortOpt.map(keys => NonTerminal("Sort" :: nt, None,
                (keys.map { case (expr, ot) => Terminal("Key" :: "Sort" :: nt, Some(expr.toString + " -> " + ot)) } ).toList)).getOrElse(Terminal("None" :: Nil, None)) ::
              Terminal("Limit" :: nt, Some(limitOpt.toString)) ::
              finalizerOpt.map(RJ.render(_)).getOrElse(Terminal("None" :: Nil, None)) ::
              Terminal("Scope" :: nt, Some(scopeOpt.toString)) ::
              Terminal("JsMode" :: nt, Some(jsModeOpt.toString)) ::
              Nil)

        case _ => Terminal(WorkflowTaskNodeType, Some(task.toString))
      }
    }

  /**
    Run once a task is known to be completely built.
    */
  def finish(base: DocVar, task: WorkflowTask):
      (DocVar, WorkflowTask) = task match {
    case PipelineTask(src, pipeline) =>
      // possibly toss duplicate `_id`s created by `Unwind`s
      val uwIdx = pipeline.lastIndexWhere {
        case $Unwind(_, _) => true;
        case _ => false
      }
      // we’re fine if there’s no `Unwind`, or some existing op fixes the `_id`s
      if (uwIdx == -1 ||
        pipeline.indexWhere(
          { case $Group(_, _, _)           => true
            case $Project(_, _, ExcludeId) => true
            case _                         => false
          },
          uwIdx) != -1)
        (base, task)
      else shape(pipeline) match {
        case Some(names) =>
          (DocVar.ROOT(),
            PipelineTask(
              src,
              pipeline :+
              $Project((),
                Reshape(names.map(n => n -> \/-($var(DocField(n)))).toListMap),
                ExcludeId)))

        case None =>
          (Workflow.ExprVar,
            PipelineTask(
              src,
              pipeline :+
                $Project((),
                  Reshape(ListMap(Workflow.ExprName -> \/-($var(base)))),
                  ExcludeId)))
      }
    case _ => (base, task)
  }

  private def shape(p: Pipeline): Option[List[BsonField.Name]] = {
    def src = shape(p.dropRight(1))

    p.lastOption.flatMap(_ match {
      case op: ShapePreservingF[_]                 => src

      case $Project((), Reshape(shape), _)         => Some(shape.keys.toList)
      case $Group((), Grouped(shape), _)           => Some(shape.keys.map(_.toName).toList)
      case $Unwind((), _)                          => src
      case $Redact((), _)                          => None
      case $GeoNear((), _, _, _, _, _, _, _, _, _) => src.map(_ :+ BsonField.Name("dist"))
    })
  }

  /**
   * A task that returns a necessarily small amount of raw data.
   */
  final case class PureTask(value: Bson) extends WorkflowTask

  /**
   * A task that merely sources data from some specified collection.
   */
  final case class ReadTask(value: Collection) extends WorkflowTask

  /**
   * A task that executes a Mongo read query.
   */
  final case class QueryTask(
    source: WorkflowTask,
    query: FindQuery,
    skip: Option[Int],
    limit: Option[Int])
      extends WorkflowTask

  /**
   * A task that executes a Mongo pipeline aggregation.
   */
  final case class PipelineTask(source: WorkflowTask, pipeline: Pipeline)
      extends WorkflowTask

  /**
   * A task that executes a Mongo map/reduce job.
   */
  final case class MapReduceTask(source: WorkflowTask, mapReduce: MapReduce)
      extends WorkflowTask

  /**
   * A task that executes a sequence of other tasks, one at a time, collecting
   * the results in the same collection. The first task must produce a new
   * collection, and the remaining tasks must be able to merge their results
   * into an existing collection, hence the types.
   */
  final case class FoldLeftTask(head: WorkflowTask, tail: NonEmptyList[MapReduceTask])
      extends WorkflowTask

  /**
   * A task that evaluates some code on the server. The JavaScript function
   * must accept two parameters: the source collection, and the destination
   * collection.
   */
  // final case class EvalTask(source: WorkflowTask, code: Js.FuncDecl)
  //     extends WorkflowTask
}
