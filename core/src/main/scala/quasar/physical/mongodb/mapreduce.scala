/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fs.Positive
import quasar.javascript._

import com.mongodb._

import monocle.macros.{GenLens}

import org.bson.conversions.{Bson => ToBson}

import scalaz._, Scalaz._

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
        Some("out" -> out.getOrElse(WithAction(Action.Replace, None, None)).bson(dst)) ::
        selection.map("query" -> _.bson) ::
        limit.map("limit" -> Bson.Int64(_)) ::
        finalizer.map("finalize" -> Bson.JavaScript(_)) ::
        (if (scope.isEmpty) None else Some("scope" -> Bson.Doc(scope))) ::
        verbose.map("verbose" -> Bson.Bool(_)) ::
        Nil).foldMap(_.toList): _*))
}

object MapReduce {
  type Scope = ListMap[String, Bson]

  sealed trait Output {
    def outputTypeEnum: com.mongodb.MapReduceCommand.OutputType
    def outputType: String = outputTypeEnum.name.toLowerCase
    def bson(dst: Collection): Bson
  }

  sealed trait Action {
    def nonAtomic: Option[Boolean]
  }
  object Action {
    /** Replace any existing documents in the destination collection with the
      * result of the map reduce.
      */
    final case object Replace extends Action {
      def nonAtomic = None
    }
    /** Merge the result of the map reduce with the existing contents of the
      * output collection.
      */
    final case class Merge(nonAtomic: Option[Boolean]) extends Action
    /** The name would suggest the output of the map reduce is reduced, using
      * the reducer function, with any existing contents of the output
      * collection, but the MongoDB docs are incomplete here.
      */
    final case class Reduce(nonAtomic: Option[Boolean]) extends Action
  }

  final case class WithAction(
    action:    Action,
    db:        Option[String],
    sharded:   Option[Boolean]) extends Output {

    def outputTypeEnum = action match {
      case Action.Replace   => MapReduceCommand.OutputType.REPLACE
      case Action.Merge(_)  => MapReduceCommand.OutputType.MERGE
      case Action.Reduce(_) => MapReduceCommand.OutputType.REDUCE
    }

    def bson(dst: Collection) = Bson.Doc(ListMap(
      (Some(outputType -> Bson.Text(dst.collectionName)) ::
        db.map("db" -> Bson.Text(_)) ::
        sharded.map("sharded" -> Bson.Bool(_)) ::
        action.nonAtomic.map("nonAtomic" -> Bson.Bool(_)) ::
        Nil
      ).foldMap(_.toList): _*))
  }

  final case object Inline extends Output {
    def outputTypeEnum = MapReduceCommand.OutputType.INLINE
    def bson(dst: Collection) = Bson.Doc(ListMap("inline" -> Bson.Int64(1)))
  }

  /** Configuration parameters for MapReduce operations
    *
    * @param finalizer JavaScript function applied to the output after the
    *                  `reduce` function.
    * @param inputFilter Query selector to apply to input documents
    * @param inputLimit Limit the number of input documents to `map`
    * @param map The mapping function
    * @param reduce The reducing function
    * @param scope Global variables made available to the `map`, `reduce` and
    *              `finalizer` functions.
    * @param inputSortCriteria Criteria to use to sort the input documents.
    * @param useJsMode Whether to avoid converting intermediate values to
    *                  BSON, leaving them as JavaScript objects instead.
    *                  Setting this to `true` has implications on the size of
    *                  the input, see the MongoDB `mapReduce` documentation
    *                  for details.
    * @param verboseResults Whether to include additional information, such
    *                       as timing, in the results.
    */
  final case class Config(
    finalizer: Option[String],
    inputFilter: Option[ToBson],
    inputLimit: Option[Positive],
    map: String,
    reduce: String,
    scope: Option[ToBson],
    sort: Option[ToBson],
    useJsMode: Boolean,
    verboseResults: Boolean
  )

  /** Action to apply to output collection.
    *
    * @param action The action to take if the output collection exists.
    * @param databaseName the database containing the output collection,
    *                     defaulting to the source database
    * @param shardOutputCollection whether the output collection should be
    *                              sharded (the output database must support
    *                              sharding).
    */
  final case class ActionedOutput(
    action: Action,
    databaseName: Option[String],
    shardOutputCollection: Option[Boolean]
  )

  /** Output collection for non-inline map-reduce jobs. */
  final case class OutputCollection(
    collectionName: String,
    withAction: Option[ActionedOutput]
  )

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
