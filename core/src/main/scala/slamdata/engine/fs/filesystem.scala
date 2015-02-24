package slamdata.engine.fs

import scalaz.\/
import scalaz.concurrent._
import scalaz.stream._

import argonaut._, Argonaut._

import slamdata.engine.{Data, DataCodec}

case class WriteError(value: Data, hint: Option[String] = None) extends slamdata.engine.Error {
  def message = hint.getOrElse("error writing data") + "; value: " + value
}
object WriteError {
  implicit val Encode = EncodeJson[WriteError]( e =>
    Json("data"   := DataCodec.Precise.encode(e.value),
         "detail" := e.hint.getOrElse("")))
}

trait FileSystem {
  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, Data]

  final def scanAll(path: Path) = scan(path, None, None)

  final def scanTo(path: Path, limit: Long) = scan(path, None, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, Some(offset), None)

  def count(path: Path): Task[Long]

  /**
   Save a collection of documents at the given path, replacing any previous contents,
   atomically. If any error occurs while consuming input values, nothing is written
   and any previous values are unaffected.
   */
  def save(path: Path, values: Process[Task, Data]): Task[Unit]

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
  }

  case class FileNotFoundError(path: Path) extends slamdata.engine.Error {
    def message = "No file/dir at path: " + path
  }
}
