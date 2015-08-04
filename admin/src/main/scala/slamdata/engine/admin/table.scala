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

package slamdata.engine.admin

import slamdata.Predef._
import scala.AnyRef
import scalaz._
import Scalaz._
import scalaz.stream.{async => _, _}

import slamdata.engine.{Backend, Errors, ResultPath, Data, DataCodec}; import Backend._; import Errors._
import slamdata.engine.repl.Prettify

import scala.swing.{Swing, Alignment, Label, Table}
import SwingUtils._
import java.awt.Color

class CollectionTableModel(fs: Backend, path: ResultPath) extends javax.swing.table.AbstractTableModel {
  val ChunkSize = 100

  async(fs.count(path.path).run)(_.fold(
    println,
    _.fold(println,
      rows => {
        collectionSize = Some((rows min Int.MaxValue).toInt)
        fireTableStructureChanged
      })))

  var collectionSize: Option[Int] = None
  var results: PartialResultSet[Data] = PartialResultSet(ChunkSize)
  var columns: List[Prettify.Path] = Nil

  def getColumnCount: Int = columns.length max 1
  def getRowCount: Int = collectionSize.getOrElse(1)
  // NB: in case the table does not use a custom renderer
  def getValueAt(row: Int, column: Int): java.lang.Object =
    getData(row, column) match {
      case LazyValue.Loading => "loading"
      case LazyValue.Missing => ""
      case LazyValue.Value(data) => Prettify.render(data).value
    }
  override def getColumnName(column: Int) =
    columns.drop(column).headOption.fold("value")(_.label)  // TODO: prune common prefix

  def getData(row: Int, column: Int): LazyValue[Data] =
    collectionSize match {
      case None => LazyValue.Loading
      case _ =>
        results.get(row) match {
          case -\/(-\/(chunk)) => {
            load(chunk)
            LazyValue.Loading
          }
          case -\/(\/-(_)) => LazyValue.Missing
          case \/-(data) => (for {
              p <- columns.drop(column).headOption
              v <- Prettify.flatten(data).get(p)
            } yield v).map(v => LazyValue.Value(v)).getOrElse(LazyValue.Missing)
        }
    }

  /**
    Get the value of every cell in the table as a sequence of rows, the first
    row of which is the column names (as currently seen in the table). The
    values are read directly from the source collection, and not cached for
    display.
    */
  def getAllValues: Process[ResTask, List[String]] = {
    // TODO: handle columns not discovered yet?
    val currentColumns = columns
    val header = currentColumns.map(_.toString)
      (Process.emit(header): Process[ResTask, List[String]]) ++ (fs.scanAll(path.path).map { data =>
        currentColumns.map(p => Prettify.flatten(data).get(p).fold(
          "")(
          Prettify.render(_).value))
      }: Process[ResTask, List[String]])
  }

  def cleanup: Backend.PathTask[Unit] = path match {
    case ResultPath.Temp(path) => for {
      _ <- fs.delete(path)
      _ = println("Deleted temp collection: " + path)
    } yield ()
    case ResultPath.User(_) => ().point[Backend.PathTask]
  }

  private def load(chunk: Chunk) = {
    def fireUpdated = fireTableRowsUpdated(chunk.firstRow, chunk.firstRow + chunk.size - 1)

    results = results.withLoading(chunk)
    fireUpdated

    async(fs.scan(path.path, chunk.firstRow, Some(chunk.size)).runLog.run)(_.fold(
      println,
      _.fold(
        println,
        rows => {
          val data = rows.toVector

          results = results.withRows(chunk, data)

          val newColumns = data.map(d => Prettify.flatten(d).keys.toList)
          val merged = newColumns.foldLeft[List[Prettify.Path]](columns) { case (cols, nc) => Prettify.mergePaths(cols, nc) }
          if (merged != columns) {
            columns = merged
            fireTableStructureChanged
          }
          else fireUpdated
        })))
  }
}

class DataCellRenderer extends Table.AbstractRenderer[AnyRef, Label](new Label) {
  component.border = Swing.EmptyBorder(0, 2, 0, 2)

  def configure(table: Table, isSelected: Boolean, hasFocus: Boolean, a: AnyRef, row: Int, column: Int): Unit = {
    val model = table.model.asInstanceOf[CollectionTableModel]
    model.getData(row, table.peer.convertColumnIndexToModel(column)) match {
      case LazyValue.Loading =>
        component.text = if (column == 0) "loading" else ""
        component.foreground = Color.GRAY
        component.horizontalAlignment = Alignment.Left
      case LazyValue.Missing =>
        component.text = ""
      case LazyValue.Value(data) =>
        Prettify.render(data) match {
          case Prettify.Aligned.Left(str) =>
            component.text = str
            component.foreground = table.foreground
            component.horizontalAlignment = Alignment.Left
          case Prettify.Aligned.Right(str) =>
            component.text = str
            component.foreground = new Color(0, 0, 0x7f)
            component.horizontalAlignment = Alignment.Right
        }
    }
  }
}

sealed trait LazyValue[+A]
object LazyValue {
  final case object Loading extends LazyValue[Nothing]
  final case object Missing extends LazyValue[Nothing]
  final case class Value[A](value: A) extends LazyValue[A]
}

final case class Chunk(chunkIndex: Int, firstRow: Int, size: Int)
final case object Loading
final case class PartialResultSet[A](chunkSize: Int, chunks: Map[Int, Loading.type \/ Vector[A]] = Map[Int, Loading.type \/ Vector[A]]()) {
  /**
   * One of:
   * - a chunk specifying rows that need to be loaded
   * - Loading, signifying the requested row is already being loaded
   * - the requested row
   */
  def get(row: Int): Chunk \/ Loading.type \/ A = {
    val (chunk, offset) = chunkIndex(row)
    chunks.get(chunk) match {
      case None => -\/(-\/(Chunk(chunk, chunk*chunkSize, chunkSize)))
      case Some(\/-(rows)) => \/-(rows(offset))
      case Some(_) => -\/(\/-(Loading))
    }
  }

  def withLoading(chunk: Chunk) = copy(chunks = chunks + (chunk.chunkIndex -> -\/(Loading)))

  def withRows(chunk: Chunk, rows: Vector[A]) = copy(chunks = chunks + (chunk.chunkIndex -> \/-(rows)))

  private def chunkIndex(row: Int): (Int, Int) = (row / chunkSize) -> (row % chunkSize)
}
