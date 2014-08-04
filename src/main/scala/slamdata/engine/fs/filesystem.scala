package slamdata.engine.fs

import scalaz.\/
import scalaz.concurrent._
import scalaz.stream._

import argonaut._

case class RenderedJson(value: String) {
  def toJson: String \/ Json = JsonParser.parse(value)

  override def toString = value
}

trait FileSystem {
  def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson]

  final def scanAll(path: Path) = scan(path, None, None)

  final def scanTo(path: Path, limit: Long) = scan(path, None, Some(limit))

  final def scanFrom(path: Path, offset: Long) = scan(path, Some(offset), None)

  def delete(path: Path): Task[Unit]

  def ls(dir: Path): Task[List[Path]]

  def ls: Task[List[Path]] = ls(Path.Root)
}

object FileSystem {
  val Null = new FileSystem {
    def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = Process.halt

    def delete(path: Path): Task[Unit] = Task.now(())

    def ls(dir: Path): Task[List[Path]] = Task.now(Nil)
  }
  
  case class FileNotFoundError(path: Path) extends slamdata.engine.Error {
    def message = "No file/dir at path: " + path
  }
}