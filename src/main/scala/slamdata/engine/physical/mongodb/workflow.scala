package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{ShowTree, NodeRenderer, Terminal, NonTerminal}

/**
 * A workflow consists of one or more tasks together with the collection
 * where the results of executing the workflow will be placed.
 */
sealed case class Workflow(task: WorkflowTask)

object Workflow {
  implicit object WorkflowNodeRenderer extends NodeRenderer[Workflow] {
    def render(wf: Workflow) = NonTerminal("Workflow", WorkflowTask.WorkflowTaskNodeRenderer.render(wf.task) :: Nil)
  }
  
  implicit def WorkflowShow = new Show[Workflow] {
    override def show(v: Workflow) = ShowTree.showTree(v)
  }
}

sealed trait WorkflowTask

object WorkflowTask {
  implicit val WorkflowTaskShow = new Show[WorkflowTask] {
    override def show(v: WorkflowTask): Cord = Cord(v.toString) // TODO!!!!
  }

  implicit object WorkflowTaskNodeRenderer extends NodeRenderer[WorkflowTask] {
    def render(task: WorkflowTask) = task match {
      case PipelineTask(source, pipeline) => NonTerminal("PipelineTask", 
                                                          Terminal(source.toString) :: 
                                                          implicitly[NodeRenderer[Pipeline]].render(pipeline) :: 
                                                          Nil)
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
  case class QueryTask(source: WorkflowTask, query: FindQuery, skip: Option[Int], limit: Option[Int]) extends WorkflowTask

  /**
   * A task that executes a Mongo pipeline aggregation.
   */
  case class PipelineTask(source: WorkflowTask, pipeline: Pipeline) extends WorkflowTask

  /**
   * A task that executes a Mongo map/reduce job.
   */
  case class MapReduceTask(source: WorkflowTask, mapReduce: MapReduce) extends WorkflowTask

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
  // case class EvalTask(source: WorkflowTask, code: Js.FuncDecl) extends WorkflowTask
}