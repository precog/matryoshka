package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import scalaz._
import monocle.Macro._
import com.mongodb._

import slamdata.engine.javascript._

case class MapReduce(
  map:        Js.Expr, // "function if (...) emit(...) }"
  reduce:     Js.Expr, // "function (key, values) { ...; return ... }"
  out:        Option[MapReduce.Output] = None,
  selection:  Option[Selector] = None,
  inputSort:  Option[NonEmptyList[(BsonField, SortType)]] = None,
  limit:      Option[Long] = None,
  finalizer:  Option[Js.Expr] = None, // "function (key, reducedValue) { ...; return ... }"
  scope:      MapReduce.Scope = ListMap(),
  jsMode:     Option[Boolean] = None,
  verbose:    Option[Boolean] = None) {

  import MapReduce._

  def bson(dst: Collection): Bson.Doc =
    Bson.Doc(ListMap(
      (// "map" -> Bson.JavaScript(map) ::
       //  "reduce" -> Bson.JavaScript(reduce) ::
        Some("out" -> out.getOrElse(WithAction()).bson(dst)) ::
        selection.map("query" -> _.bson) ::
        limit.map("limit" -> Bson.Int64(_)) ::
        finalizer.map("finalize" -> Bson.JavaScript(_)) ::
        (if (scope.isEmpty) None else Some("scope" -> Bson.Doc(scope))) ::
        verbose.map("verbose" -> Bson.Bool(_)) ::
        Nil).flatten: _*))
}

object MapReduce {
  type Scope = ListMap[String, Bson]

  sealed trait Output {
    def outputTypeEnum: com.mongodb.MapReduceCommand.OutputType
    def outputType: String = outputTypeEnum.name.toLowerCase
    def bson(dst: Collection): Bson
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

    def bson(dst: Collection) = Bson.Doc(ListMap(
      (Some(outputType -> Bson.Text(dst.name)) ::
        db.map("db" -> Bson.Text(_)) ::
        sharded.map("sharded" -> Bson.Bool(_)) ::
        nonAtomic.map("nonAtomic" -> Bson.Bool(_)) ::
        Nil
      ).flatten: _*))
  }

  case object Inline extends Output {
    def outputTypeEnum = MapReduceCommand.OutputType.INLINE
    def bson(dst: Collection) = Bson.Doc(ListMap("inline" -> Bson.Int64(1)))
  }

  val _map       = mkLens[MapReduce, Js.Expr]("map")
  val _reduce    = mkLens[MapReduce, Js.Expr]("reduce")
  val _out       = mkLens[MapReduce, Option[Output]]("out")
  val _selection = mkLens[MapReduce, Option[Selector]]("selection")
  val _inputSort = mkLens[MapReduce, Option[NonEmptyList[(BsonField, SortType)]]]("inputSort")
  val _limit     = mkLens[MapReduce, Option[Long]]("limit")
  val _finalizer = mkLens[MapReduce, Option[Js.Expr]]("finalizer")
  val _scope     = mkLens[MapReduce, Scope]("scope")
  val _jsMode    = mkLens[MapReduce, Option[Boolean]]("jsMode")
  val _verbose   = mkLens[MapReduce, Option[Boolean]]("verbose")
}
