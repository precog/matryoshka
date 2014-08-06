package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import scalaz._
import com.mongodb._

case class MapReduce(
  map:        Js.Expr, // "function if (...) emit(...) }"
  reduce:     Js.Expr, // "function (key, values) { ...; return ... }"
  out:        Option[MapReduce.Output] = None,
  selection:  Option[Selector] = None,
  inputSort:  Option[NonEmptyList[(BsonField, SortType)]] = None,
  limit:      Option[Int] = None,
  finalizer:  Option[Js.Expr] = None, // "function (key, reducedValue) { ...; return ... }"
  scope:      Option[Map[String, Bson]] = None,
  jsMode:     Option[Boolean] = None,
  verbose:    Option[Boolean] = None) {

  def bson(dst: Collection): Bson.Doc =
    Bson.Doc(ListMap(
      (// "map" -> Bson.JavaScript(map) ::
       //  "reduce" -> Bson.JavaScript(reduce) ::
        out.map(o =>
          "out" -> Bson.Doc(ListMap(
            o.outputType -> Bson.Text(dst.name)))) ::
        selection.map(s =>
          "query" -> s.bson) ::
        limit.map(l =>
          "limit" -> Bson.Int32(l)) ::
        finalizer.map(f =>
          "finalize" -> Bson.JavaScript(f)) ::
        verbose.map(v =>
          "verbose" -> Bson.Bool(v)) ::
        Nil
      ).flatten: _*))
}

object MapReduce {
  sealed trait Output {
    def outputTypeEnum: com.mongodb.MapReduceCommand.OutputType
    def outputType: String = outputTypeEnum.name.toLowerCase
  }

  sealed trait Action
  object Action {
    case object Replace extends Action
    case object Merge extends Action
    case object Reduce extends Action
  }

  case class WithAction(
    action:    Action = Action.Replace,
    db:        Option[String] = None,
    sharded:   Option[Boolean] = None,
    nonAtomic: Option[Boolean] = None) extends Output {

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
