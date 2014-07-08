package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.{RenderTree, Terminal, NonTerminal}

/**
 * A workflow consists of one or more tasks together with the collection
 * where the results of executing the workflow will be placed.
 */
sealed case class Workflow(task: WorkflowTask)

object Workflow {
  implicit def WorkflowRenderTree(implicit RT: RenderTree[WorkflowTask]) =
    new RenderTree[Workflow] {
      def render(wf: Workflow) =
        NonTerminal("Workflow", List(RT.render(wf.task)))
    }

  val NativeWorkflowShow: Show[Workflow] = {
    // override the normal pipeline op implicit to show the op's bson representation:
    implicit val ro: RenderTree[PipelineOp] = new RenderTree[PipelineOp] {
      override def render(v: PipelineOp) = Terminal(v.bson.repr.toString)
    }
    
    // override the workflowtask implicit to produce valid mongo shell syntax, at least in one case:
    implicit def rt: RenderTree[WorkflowTask] = new RenderTree[WorkflowTask] {
      override def render(v: WorkflowTask) = v match {
        case WorkflowTask.PipelineTask(WorkflowTask.ReadTask(Collection(name)), Pipeline(ops)) => 
          Terminal("db." + name + ".aggregate([\n  " +
                    ops.map(_.bson.repr.toString).mkString(",\n  ") +
                    "\n])")
        case _ => WorkflowTask.WorkflowTaskRenderTree.render(v)
      }
    }

    RenderTreeToShow(WorkflowRenderTree)
  }
}

sealed trait WorkflowTask

object WorkflowTask {
  implicit def WorkflowTaskRenderTree(implicit RP: RenderTree[Pipeline]) =
    new RenderTree[WorkflowTask] {
      def render(task: WorkflowTask) = task match {
        case PipelineTask(source, pipeline) =>
          NonTerminal(
            "PipelineTask",
            List(Terminal(source.toString), RP.render(pipeline)))
        case _ => Terminal(task.toString)
    }
  }

  /**
   * A task that returns a necessarily small amount of raw data.
   */
  case class PureTask(value: Bson) extends WorkflowTask

  /**
   * A task that merely sources data from some specified collection.
   */
  case class ReadTask(value: Collection) extends WorkflowTask

  /**
   * A task that executes a Mongo read query.
   */
  case class QueryTask(
    source: WorkflowTask,
    query: FindQuery,
    skip: Option[Int],
    limit: Option[Int])
      extends WorkflowTask

  /**
   * A task that executes a Mongo pipeline aggregation.
   */
  case class PipelineTask(source: WorkflowTask, pipeline: Pipeline)
      extends WorkflowTask

  /**
   * A task that executes a Mongo map/reduce job.
   */
  case class MapReduceTask(source: WorkflowTask, mapReduce: MapReduce)
      extends WorkflowTask

  /**
   * A task that executes a sequence of other tasks, one at a time, collecting
   * the results in the same collection.
   */
  case class FoldLeftTask(steps: NonEmptyList[WorkflowTask])
      extends WorkflowTask

  /**
   * A task that executes a number of others in parallel and merges them
   * into the same collection.
   */
  case class JoinTask(steps: NonEmptyList[WorkflowTask]) extends WorkflowTask

  /**
   * A task that evaluates some code on the server. The JavaScript function
   * must accept two parameters: the source collection, and the destination 
   * collection.
   */
  // case class EvalTask(source: WorkflowTask, code: Js.FuncDecl)
  //     extends WorkflowTask
}
