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

/** Configuration parameters for MapReduce operations
  *
  * @param map The mapping function
  * @param reduce The reducing function
  * @param selection Query selector to apply to input documents
  * @param inputSort Criteria to use to sort the input documents.
  * @param limit Limit the number of input documents to `map`
  * @param finalizer JavaScript function applied to the output after the
  *                  `reduce` function.
  * @param scope Global variables made available to the `map`, `reduce` and
  *              `finalizer` functions.
  * @param jsMode Whether to avoid converting intermediate values to
  *               BSON, leaving them as JavaScript objects instead.
  *               Setting this to `true` has implications on the size of
  *               the input, see the MongoDB `mapReduce` documentation
  *               for details.
  * @param verbose Whether to include additional information, such
  *                as timing, in the results.
  */
@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.DefaultArguments"))
final case class MapReduce(
  map:       Js.Expr, // "function if (...) emit(...) }"
  reduce:    Js.Expr, // "function (key, values) { ...; return ... }"
  selection: Option[Selector] = None,
  inputSort: Option[NonEmptyList[(BsonField, SortType)]] = None,
  limit:     Option[Long] = None,
  finalizer: Option[Js.Expr] = None, // "function (key, reducedValue) { ...; return ... }"
  scope:     MapReduce.Scope = ListMap(),
  jsMode:    Option[Boolean] = None,
  verbose:   Option[Boolean] = None) {

  import MapReduce._

  def inlineBson: Bson.Doc =
    toBson(Bson.Doc(ListMap("inline" -> Bson.Int64(1))))

  def toCollBson(dst: OutputCollection): Bson.Doc =
    toBson(dst.bson)

  ////

  private def toBson(out: Bson): Bson.Doc = {
    def sortBson(xs: NonEmptyList[(BsonField, SortType)]): Bson.Doc =
      Bson.Doc(ListMap(xs.list.map(_ bimap (_.asText, _.bson)): _*))

    Bson.Doc(ListMap(("out" -> out) :: List(
      selection        map  ("query"    -> _.bson),
      inputSort        map  ("sort"     -> sortBson(_)),
      limit            map  ("limit"    -> Bson.Int64(_)),
      finalizer        map  ("finalize" -> Bson.JavaScript(_)),
      scope.nonEmpty option ("scope"    -> Bson.Doc(scope)),
      jsMode           map  ("jsMode"   -> Bson.Bool(_)),
      verbose          map  ("verbose"  -> Bson.Bool(_))
    ).unite: _*))
  }
}

object MapReduce {
  type Scope = ListMap[String, Bson]

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

    /** Returns the field name that represents the given `Action` in a
      * map-reduce BSON document.
      */
    def bsonFieldName(act: Action): String =
      (act match {
        case Replace   => MapReduceCommand.OutputType.REPLACE
        case Merge(_)  => MapReduceCommand.OutputType.MERGE
        case Reduce(_) => MapReduceCommand.OutputType.REDUCE
      }).name.toLowerCase
  }

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
  ) {
    def bson(collName: String): Bson.Doc =
      Bson.Doc(ListMap((Action.bsonFieldName(action) -> Bson.Text(collName)) :: List(
        databaseName          map ("db"        -> Bson.Text(_)),
        shardOutputCollection map ("sharded"   -> Bson.Bool(_)),
        action.nonAtomic      map ("nonAtomic" -> Bson.Bool(_))
      ).unite: _*))
  }

  /** Output collection for non-inline map-reduce jobs. */
  final case class OutputCollection(
    collectionName: String,
    withAction: Option[ActionedOutput]
  ) {
    def bson: Bson =
      withAction.fold[Bson](Bson.Text(collectionName))(_ bson collectionName)
  }

  val _map       = GenLens[MapReduce](_.map)
  val _reduce    = GenLens[MapReduce](_.reduce)
  val _selection = GenLens[MapReduce](_.selection)
  val _inputSort = GenLens[MapReduce](_.inputSort)
  val _limit     = GenLens[MapReduce](_.limit)
  val _finalizer = GenLens[MapReduce](_.finalizer)
  val _scope     = GenLens[MapReduce](_.scope)
  val _jsMode    = GenLens[MapReduce](_.jsMode)
  val _verbose   = GenLens[MapReduce](_.verbose)
}
