package slamdata.engine.fs

import scalaz.\/
import scalaz.concurrent._
import scalaz.stream._

import argonaut._, Argonaut._

case class RenderedJson(value: String) {
  def toJson: String \/ Json = JsonParser.parse(value)

  override def toString = value
}

case class JsonWriteError(value: RenderedJson, hint: Option[String] = None) extends slamdata.engine.Error {
  def message = hint.getOrElse("error writing json") + "; value: " + value
}
object JsonWriteError {
  implicit val Encode = EncodeJson[JsonWriteError]( e => 
    Json("json"   := e.value.value, 
         "detail" := e.hint.getOrElse("")))
}

trait FileSystem {
  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson]

  final def scanAll(path: Path) = scan(path, None, None)

  final def scanTo(path: Path, limit: Long) = scan(path, None, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, Some(offset), None)

  /**
   Save a collection of documents at the given path, replacing any previous contents,
   atomically. If any error occurs while consuming input values, nothing is written 
   and any previous values are unaffected.
   */
  def save(path: Path, values: Process[Task, RenderedJson]): Task[Unit]

  /**
   Add values to a possibly existing collection. May write some values and not others,
   due to bad input or problems on the backend side. The result stream yields an error 
   for each input value that is not written, or no values at all.
   */
  def append(path: Path, values: Process[Task, RenderedJson]): Process[Task, JsonWriteError]

  def move(src: Path, dst: Path): Task[Unit]

  def delete(path: Path): Task[Unit]

  def ls(dir: Path): Task[List[Path]]

  def ls: Task[List[Path]] = ls(Path.Root)
}

object FileSystem {
  val Null = new FileSystem {
    def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = Process.halt

    def save(path: Path, values: Process[Task, RenderedJson]) = Task.now(())

    def append(path: Path, values: Process[Task, RenderedJson]) = Process.halt

    def delete(path: Path): Task[Unit] = Task.now(())

    def move(src: Path, dst: Path) = Task.now(())

    def ls(dir: Path): Task[List[Path]] = Task.now(Nil)
  }
  
  case class FileNotFoundError(path: Path) extends slamdata.engine.Error {
    def message = "No file/dir at path: " + path
  }
}