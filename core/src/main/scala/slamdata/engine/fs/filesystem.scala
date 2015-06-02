package slamdata.engine.fs

import scalaz._
import Scalaz._
import scalaz.concurrent._
import scalaz.stream._

import argonaut._, Argonaut._

import slamdata.engine.{Data, DataCodec}

final case class WriteError(value: Data, hint: Option[String]) extends slamdata.engine.Error {
  def message = hint.getOrElse("error writing data") + "; value: " + value
}
object WriteError {
  implicit val Encode = EncodeJson[WriteError]( e =>
    Json("data"   := DataCodec.Precise.encode(e.value),
         "detail" := e.hint.getOrElse("")))
}

final case class InvalidOffsetError(value: Long) extends slamdata.engine.Error {
  def message = "invalid offset: " + value + " (must be >= 0)"
}
final case class InvalidLimitError(value: Long) extends slamdata.engine.Error {
  def message = "invalid limit: " + value + " (must be >= 1)"
}

trait FileSystem {
  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, Data]

  final def scanAll(path: Path) = scan(path, None, None)

  final def scanTo(path: Path, limit: Long) = scan(path, None, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, Some(offset), None)

  def count(path: Path): Task[Long]

  /**
    Save a collection of documents at the given path, replacing any previous
    contents, atomically. If any error occurs while consuming input values,
    nothing is written and any previous values are unaffected.
    */
  def save(path: Path, values: Process[Task, Data]): Task[Unit]

  /**
    Create a new collection of documents at the given path.
    */
  def create(path: Path, values: Process[Task, Data]) =
    exists(path).flatMap(ex =>
      if (ex) Task.fail(new RuntimeException(path.shows + " can’t be created, because it already exists"))
      else save(path, values))

  /**
    Replaces a collection of documents at the given path. If any error occurs,
    the previous contents should be unaffected.
  */
  def replace(path: Path, values: Process[Task, Data]) =
    exists(path).flatMap(ex =>
      if (ex) save(path, values)
      else Task.fail(new RuntimeException(path.shows + " can’t be replaced, because it doesn’t exist")))

  /**
   Add values to a possibly existing collection. May write some values and not others,
   due to bad input or problems on the backend side. The result stream yields an error
   for each input value that is not written, or no values at all.
   */
  def append(path: Path, values: Process[Task, Data]): Process[Task, WriteError]

  def move(src: Path, dst: Path): Task[Unit]

  def delete(path: Path): Task[Unit]

  def ls(dir: Path): Task[List[Path]]

  def ls: Task[List[Path]] = ls(Path.Root)

  def exists(path: Path): Task[Boolean] =
    if (path == Path.Root) Task.now(true)
    else ls(path.parent).map(p => p.map(path.parent ++ _) contains path)

  def defaultPath: Path
}

object FileSystem {
  val Null = new FileSystem {
    def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, Data] = Process.halt

    def count(path: Path) = Task.now(0)

    def save(path: Path, values: Process[Task, Data]) = Task.now(())

    def append(path: Path, values: Process[Task, Data]) = Process.halt

    def delete(path: Path): Task[Unit] = Task.now(())

    def move(src: Path, dst: Path) = Task.now(())

    def ls(dir: Path): Task[List[Path]] = Task.now(Nil)

    def defaultPath = Path(".")
  }

  final case class FileNotFoundError(path: Path) extends slamdata.engine.Error {
    def message = "No file/dir at path: " + path
  }
}
