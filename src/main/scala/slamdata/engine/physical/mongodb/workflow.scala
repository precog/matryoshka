package slamdata.engine.physical.mongodb

import scalaz.NonEmptyList

/**
 * A workflow consists of one or more tasks together with the collection
 * where the results of executing the workflow will be placed.
 */
sealed case class Workflow(task: Task, dest: Collection)

sealed trait Task

object Task {
  /**
   * A task that returns a necessarily small amount of raw data.
   */
  case class PureTask(value: Bson) extends Task

  /**
   * A task that merely sources data from some specified collection.
   */
  case class ReadTask(value: Collection) extends Task

  /**
   * A task that executes a Mongo read query.
   */
  case class QueryTask(source: Task, query: Query) extends Task

  /**
   * A task that executes a Mongo pipeline aggregation.
   */
  case class PipelineTask(source: Task, pipeline: Pipeline) extends Task

  /**
   * A task that executes a Mongo map/reduce job.
   */
  case class MapReduceTask(source: Task, mapReduce: MapReduce) extends Task

  /**
   * A task that executes a number of others in parallel and merges them
   * into the same collection.
   */
  case class JoinTask(steps: NonEmptyList[Task]) extends Task

  /**
   * A task that evaluates some code on the server. The JavaScript function
   * must accept two parameters: the source collection, and the destination 
   * collection.
   */
  // case class EvalTask(source: Task, code: Js.FuncDecl) extends Task
}