package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

/**
 * A workflow consists of one or more tasks together with the collection
 * where the results of executing the workflow will be placed.
 */
sealed case class Workflow(task: WorkflowTask)

object Workflow {
  private [mongodb] type WorkflowNode = Workflow \/ WorkflowTask \/ Pipeline.PipelineNode
  
  private [mongodb] def toTree(node: WorkflowNode): Tree[WorkflowNode] = {
    def asNode(children: List[WorkflowNode]) : Tree[WorkflowNode] = 
      Tree.node(node, children.map(toTree).toStream)
    
    node match {
      case -\/ (-\/ (Workflow(task))) => asNode(-\/ (\/- (task)) :: Nil)
      case -\/( \/- (WorkflowTask.PipelineTask(source, pipeline))) => asNode(-\/ (\/- (source)) :: \/- (-\/ (pipeline)) :: Nil)
      case -\/( \/- (task)) => Tree.leaf(node)
      case \/- (pipelineNode) => {
        def wrapRight[A,B](t: Tree[B]): Tree[A \/ B] = 
          Tree.node(\/- (t.rootLabel), t.subForest.map(st => wrapRight(st): Tree[A \/ B]))
        wrapRight(Pipeline.toTree(pipelineNode))
      }
    }
  }

  private [mongodb] implicit def WorkflowNodeShow = new Show[WorkflowNode] {
    override def show(node: WorkflowNode): Cord =
      Cord(node match {
        case -\/( -\/ (Workflow(_))) => "Workflow"

        case -\/( \/- (WorkflowTask.PipelineTask(_, _))) => "PipelineTask"
        case -\/( \/- (task)) => task.toString
        
        case \/-(p) => Pipeline.PipelineNodeShow.shows(p)
      })
  }

  implicit def WorkflowShow = new Show[Workflow] {
    override def show(v: Workflow): Cord = {
      Cord(toTree(-\/ ( -\/ (v))).drawTree)
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