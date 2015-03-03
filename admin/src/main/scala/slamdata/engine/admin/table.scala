package slamdata.engine.admin

import scala.collection.immutable.ListMap
import scalaz._
import Scalaz._
import scalaz.stream.{async => _, _}
import scalaz.concurrent._

import argonaut._
import Argonaut._

import slamdata.engine.{ResultPath, Data, DataCodec}
import slamdata.engine.fs._
import slamdata.engine.fp._
import slamdata.engine.repl.Prettify

import scala.swing.{Swing, Alignment, Label, Table}
import SwingUtils._
import java.awt.Color

class CollectionTableModel(fs: FileSystem, path: ResultPath) extends javax.swing.table.AbstractTableModel {
  val ChunkSize = 100

  async(fs.count(path.path))(_.fold(
    err => println(err),
    rows => {
      collectionSize = Some((rows min Int.MaxValue).toInt)
      fireTableStructureChanged
    }))

  var collectionSize: Option[Int] = None
  var results: PartialResultSet[Data] = PartialResultSet(ChunkSize)
  var columns: List[Prettify.Path] = Nil

  def getColumnCount: Int = columns.length max 1
  def getRowCount: Int = collectionSize.getOrElse(1)
  // NB: in case the table does not use a custom renderer
  def getValueAt(row: Int, column: Int): Object =
    Styled.render(getData(row, column)).str
  override def getColumnName(column: Int) =
    columns.drop(column).headOption.fold("value")(_.toString)  // TODO: prune common prefix

  // None == "loading"; Left == missing from result
  def getData(row: Int, column: Int): Option[Unit \/ Data] =
    collectionSize match {
      case None => None
      case _ =>
        results.get(row) match {
          case -\/(-\/(chunk)) => {
            load(chunk)
            None
          }
          case -\/(\/-(_)) => None
          case \/-(data) => Some((for {
              p <- columns.drop(column).headOption
              v <- Prettify.flatten(data).get(p)
            } yield v) \/> (()))
        }
    }

  /**
    Get the value of every cell in the table as a sequence of rows, the first
    row of which is the column names (as currently seen in the table). The
    values are read directly from the source collection, and not cached for
    display.
    */
  def getAllValues: Process[Task, List[String]] = {
    // TODO: handle columns not discovered yet?
    val currentColumns = columns
    val header = currentColumns.map(_.toString)
    Process.emit(header) ++ fs.scanAll(path.path).map { data =>
      val map = Prettify.flatten(data)
      currentColumns.map(p => map.get(p).fold("")(d => Prettify.render(d).fold(identity, identity)))
    }
  }

  def cleanup: Task[Unit] = path match {
    case ResultPath.Temp(path) => for {
      _ <- fs.delete(path)
      _ = println("Deleted temp collection: " + path)
    } yield ()
    case ResultPath.User(_) => Task.now(())
  }

  private def load(chunk: Chunk) = {
    def fireUpdated = fireTableRowsUpdated(chunk.firstRow, chunk.firstRow + chunk.size - 1)

    results = results.withLoading(chunk)
    fireUpdated

    async(fs.scan(path.path, Some(chunk.firstRow), Some(chunk.size)).runLog)(_.fold(
      err  => println(err),
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
      }))
  }
}

class DataCellRenderer extends Table.AbstractRenderer[AnyRef, Label](new Label) {
  import Prettify._

  component.border = Swing.EmptyBorder(0, 2, 0, 2)

  def configure(table: Table, isSelected: Boolean, hasFocus: Boolean, a: AnyRef, row: Int, column: Int) {
    val model = table.model.asInstanceOf[CollectionTableModel]
    Styled.render(model.getData(row, table.peer.convertColumnIndexToModel(column))) match {
      case Styled.Loading =>
        component.text = if (column == 0) "loading" else ""
        component.foreground = Color.GRAY
        component.horizontalAlignment = Alignment.Left
      case Styled.Missing =>
        component.text = ""
      case Styled.Str(value) =>
        component.text = value
        component.foreground = table.foreground
        component.horizontalAlignment = Alignment.Left
      case Styled.Other(value) =>
        component.text = value
        component.foreground = new Color(0, 0, 0x7f)
        component.horizontalAlignment = Alignment.Right
    }
  }
}

sealed trait Styled { def str: String }
object Styled {
  case object Loading extends Styled { val str = "loading" }
  case object Missing extends Styled { val str = "" }
  case class Str(str: String) extends Styled
  case class Other(str: String) extends Styled

  def render(data: Option[Unit \/ Data]) = data match {
    case None            => Styled.Loading
    case Some(-\/(_))    => Styled.Missing
    case Some(\/-(data)) => Prettify.render(data).fold(Str.apply, Other.apply)
  }
}

case class Chunk(chunkIndex: Int, firstRow: Int, size: Int)
case object Loading
case class PartialResultSet[A](chunkSize: Int, chunks: Map[Int, Loading.type \/ Vector[A]] = Map[Int, Loading.type \/ Vector[A]]()) {
  import PartialResultSet._

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
