package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import scalaz._
import monocle.macros.{GenLens}
import com.mongodb._

import slamdata.engine.javascript._

@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.DefaultArguments"))
final case class MapReduce(
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
        Some("out" -> out.getOrElse(WithAction(Action.Replace, None, None, None)).bson(dst)) ::
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
    final case object Replace extends Action
    final case object Merge extends Action
    final case object Reduce extends Action
  }

  final case class WithAction(
    action:    Action,
    db:        Option[String],
    sharded:   Option[Boolean],
    nonAtomic: Option[Boolean]) extends Output {

    def outputTypeEnum = action match {
      case Action.Replace => MapReduceCommand.OutputType.REPLACE
      case Action.Merge   => MapReduceCommand.OutputType.MERGE
      case Action.Reduce  => MapReduceCommand.OutputType.REDUCE
    }

    def bson(dst: Collection) = Bson.Doc(ListMap(
      (Some(outputType -> Bson.Text(dst.collectionName)) ::
        db.map("db" -> Bson.Text(_)) ::
        sharded.map("sharded" -> Bson.Bool(_)) ::
        nonAtomic.map("nonAtomic" -> Bson.Bool(_)) ::
        Nil
      ).flatten: _*))
  }

  final case object Inline extends Output {
    def outputTypeEnum = MapReduceCommand.OutputType.INLINE
    def bson(dst: Collection) = Bson.Doc(ListMap("inline" -> Bson.Int64(1)))
  }

  val _map       = GenLens[MapReduce](_.map)
  val _reduce    = GenLens[MapReduce](_.reduce)
  val _out       = GenLens[MapReduce](_.out)
  val _selection = GenLens[MapReduce](_.selection)
  val _inputSort = GenLens[MapReduce](_.inputSort)
  val _limit     = GenLens[MapReduce](_.limit)
  val _finalizer = GenLens[MapReduce](_.finalizer)
  val _scope     = GenLens[MapReduce](_.scope)
  val _jsMode    = GenLens[MapReduce](_.jsMode)
  val _verbose   = GenLens[MapReduce](_.verbose)
}
