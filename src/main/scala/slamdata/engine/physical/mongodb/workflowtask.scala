package slamdata.engine.physical.mongodb

import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.{RenderTree, Terminal, NonTerminal}

import IdHandling._

/**
  A WorkflowTask approximately represents one request to MongoDB.
  */
sealed trait WorkflowTask

object WorkflowTask {
  type Pipeline = List[PipelineOp]

  implicit def WorkflowTaskRenderTree(implicit RO: RenderTree[PipelineOp], RJ: RenderTree[Js], RS: RenderTree[Selector]) =
    new RenderTree[WorkflowTask] {
      val WorkflowTaskNodeType = List("Workflow", "WorkflowTask")
  
      def render(task: WorkflowTask) = task match {
        case ReadTask(value) => Terminal(value.name, WorkflowTaskNodeType :+ "ReadTask")
        
        case PipelineTask(source, pipeline) =>
          NonTerminal(
            "",
            render(source) :: 
              NonTerminal("", pipeline.map(RO.render(_)), "Pipeline" :: Nil) ::
              Nil,
            WorkflowTaskNodeType :+ "PipelineTask")
            
        case FoldLeftTask(head, tail) =>
          NonTerminal(
            "",
            render(head) ::
              tail.map(render(_)).toList,
            WorkflowTaskNodeType :+ "FoldLeftTask")

        case MapReduceTask(source, MapReduce(map, reduce, outOpt, selectorOpt, sortOpt, limitOpt, finalizerOpt, scopeOpt, jsModeOpt, verboseOpt)) =>
          NonTerminal("",
            render(source) ::
              RJ.render(map) ::
              RJ.render(reduce) ::
              Terminal(outOpt.toString) ::
              selectorOpt.map(RS.render(_)).getOrElse(Terminal("None")) ::
              sortOpt.map(keys => NonTerminal("", (keys.map { case (expr, ot) => Terminal(expr.toString + " -> " + ot, WorkflowTaskNodeType :+ "MapReduceTask" :+ "Sort" :+ "Key") } ).toList,
                WorkflowTaskNodeType :+ "MapReduceTask" :+ "Sort")).getOrElse(Terminal("None")) ::
              Terminal(limitOpt.toString) ::
              finalizerOpt.map(RJ.render(_)).getOrElse(Terminal("None")) ::
              Terminal(scopeOpt.toString) ::
              Terminal(jsModeOpt.toString) ::
              Nil,
            WorkflowTaskNodeType :+ "MapReduceTask")

        case _ => Terminal(task.toString, WorkflowTaskNodeType)
      }
    }

  /**
    Run once a task is known to be completely built.
    */
  def finish(base: ExprOp.DocVar, task: WorkflowTask):
      (ExprOp.DocVar, WorkflowTask) = task match {
    case PipelineTask(src, pipeline) =>
      // possibly toss duplicate `_id`s created by `Unwind`s
      val uwIdx = pipeline.lastIndexWhere {
        case PipelineOp.Unwind(_) => true;
        case _ => false
      }
      // we’re fine if there’s no `Unwind`, or some existing op fixes the `_id`s
      if (uwIdx == -1 ||
        pipeline.indexWhere(
          { case PipelineOp.Group(_, _)           => true
            case PipelineOp.Project(_, ExcludeId) => true
            case _                                => false
          },
          uwIdx) != -1)
        (base, task)
      else
        (WorkflowOp.ExprVar,
          PipelineTask(
            src,
            pipeline :+
              PipelineOp.Project(PipelineOp.Reshape.Doc(ListMap(
                WorkflowOp.ExprName -> -\/(base))),
                ExcludeId)))
    case _ => (base, task)
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
   * the results in the same collection. The first task must produce a new 
   * collection, and the remaining tasks must be able to merge their results
   * into an existing collection, hence the types.
   */
  case class FoldLeftTask(head: WorkflowTask, tail: NonEmptyList[MapReduceTask])
      extends WorkflowTask

  /**
   * A task that executes a number of others in parallel and merges them
   * into the same collection.
   */
  case class JoinTask(steps: Set[WorkflowTask]) extends WorkflowTask

  /**
   * A task that evaluates some code on the server. The JavaScript function
   * must accept two parameters: the source collection, and the destination 
   * collection.
   */
  // case class EvalTask(source: WorkflowTask, code: Js.FuncDecl)
  //     extends WorkflowTask
}
