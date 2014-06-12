package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

/**
 * A workflow consists of one or more tasks together with the collection
 * where the results of executing the workflow will be placed.
 */
sealed case class Workflow(task: WorkflowTask)

object Workflow {
  implicit def WorkflowShow = new Show[Workflow] {
    override def show(v: Workflow): Cord = {
      import WorkflowTask._

      type WFNode = Workflow \/ (WorkflowTask \/ (Pipeline \/ PipelineOp))

      def toTree(node: WFNode): Tree[WFNode] = {
        def asNode(children: List[WFNode]) : Tree[WFNode] = 
          Tree.node(node, children.map(toTree).toStream)
        
        node match {
          case -\/(Workflow(task)) => asNode(\/-(-\/(task)) :: Nil)
          case \/-(-\/(PipelineTask(source, pipeline))) => asNode(\/-(-\/(source)) :: \/-(\/-(-\/(pipeline))) :: Nil)
          case \/-(\/-(-\/(Pipeline(ops)))) => asNode(ops.map(op => \/-(\/-(\/-(op)))))
          // TODO: MapReduceTask, QueryTask, JoinTask (the only other tasks with nested structure?)
          case _ => Tree.leaf(node)
        }
      }
      
      implicit def WFNodeShow = new Show[WFNode] {
        override def show(node: WFNode): Cord =
          Cord(node match {
            case -\/(Workflow(task)) => "Workflow"
            
            case \/-(-\/(PipelineTask(source, pipeline))) => "PipelineTask"
            case \/-(-\/(task)) => task.toString
            
            case \/-(\/-(-\/(Pipeline(ops)))) => "Pipeline"
            
            case \/-(\/-(\/-(op))) => op.toString
          })
      }

      Cord(toTree(-\/(v)).drawTree)
    }
  }
}

sealed trait WorkflowTask

object WorkflowTask {
  implicit val WorkflowTaskShow = new Show[WorkflowTask] {
    override def show(v: WorkflowTask): Cord = Cord(v.toString) // TODO!!!!
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