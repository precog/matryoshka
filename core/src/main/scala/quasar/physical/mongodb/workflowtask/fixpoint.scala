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

package quasar.physical.mongodb.workflowtask

import quasar.Predef._
import quasar.physical.mongodb._
import quasar.recursionschemes._

import scalaz._

object PureTask {
  def apply(bson: Bson): WorkflowTask = Fix(PureTaskF(bson))
  def unapply(obj: WorkflowTask): Option[Bson] = PureTaskF.unapply(obj.unFix)
}

object ReadTask {
  def apply(coll: Collection): WorkflowTask = Fix(ReadTaskF(coll))
  def unapply(obj: WorkflowTask): Option[Collection] =
    ReadTaskF.unapply(obj.unFix)
}

object QueryTask {
  def apply(source: WorkflowTask, query: FindQuery, skip: Option[Int], limit: Option[Int]):
      WorkflowTask =
    Fix(QueryTaskF(source, query, skip, limit))
  def unapply(obj: WorkflowTask):
      Option[(WorkflowTask, FindQuery, Option[Int], Option[Int])] =
    QueryTaskF.unapply(obj.unFix)
}

object PipelineTask {
  def apply(source: WorkflowTask, pipeline: Pipeline): WorkflowTask =
    Fix(PipelineTaskF(source, pipeline))
  def unapply(obj: WorkflowTask): Option[(WorkflowTask, Pipeline)] =
    PipelineTaskF.unapply(obj.unFix)
}

object MapReduceTask {
  import MapReduce._
  def apply(source: WorkflowTask, mapReduce: MapReduce, outAct: Option[Action]): WorkflowTask =
    Fix(MapReduceTaskF(source, mapReduce, outAct))
  def unapply(obj: WorkflowTask): Option[(WorkflowTask, MapReduce, Option[Action])] =
    MapReduceTaskF.unapply(obj.unFix)
}

object FoldLeftTask {
  def apply(head: WorkflowTask, tail: NonEmptyList[WorkflowTask]):
      WorkflowTask =
    Fix(FoldLeftTaskF(head, tail))
  def unapply(obj: WorkflowTask):
      Option[(WorkflowTask, NonEmptyList[WorkflowTask])] =
    FoldLeftTaskF.unapply(obj.unFix)
}
