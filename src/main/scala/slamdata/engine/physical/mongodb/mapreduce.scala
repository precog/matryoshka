package slamdata.engine.physical.mongodb

import scalaz._

import com.mongodb._

case class MapReduce(
  map:        Js.AnonFunDecl, // "function () { if (...) emit(...) }"
  reduce:     Js.AnonFunDecl, // "function (key, values) { ...; return ... }"
  out:        Option[Output] = None,
  selection:  Option[Selector] = None,
  inputSort:  Option[NonEmptyList[(BsonField, SortType)]] = None,
  limit:      Option[Int] = None,
  finalizer:  Option[Js.AnonFunDecl] = None, // "function (key, reducedValue) { ...; return ... }"
  scope:      Option[Map[String, Bson]] = None,
  jsMode:     Option[Boolean] = None,
  verbose:    Option[Boolean] = None)

sealed trait Output {
  def outputTypeEnum: com.mongodb.MapReduceCommand.OutputType
  def outputType: String = outputTypeEnum.name.toLowerCase
}
object Output {
  sealed trait Action
  object Action {
    case object Replace extends Action
    case object Merge extends Action
    case object Reduce extends Action
  }

  case class WithAction(
    action:     Action = Action.Replace,
    db:         Option[String] = None,
    sharded:    Option[Boolean] = None,
    nonAtomic:  Option[Boolean] = None) extends Output {

    def outputTypeEnum = action match {
      case Action.Replace => MapReduceCommand.OutputType.REPLACE
      case Action.Merge   => MapReduceCommand.OutputType.MERGE
      case Action.Reduce  => MapReduceCommand.OutputType.REDUCE
    }
  }

  case object Inline extends Output {
    def outputTypeEnum = MapReduceCommand.OutputType.INLINE
  }
}
