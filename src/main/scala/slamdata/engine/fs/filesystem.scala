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

  def ls(dir: Path): Task[Option[List[Path]]]

  def ls: Task[List[Path]] = Task.delay(ls(Path.Root).run.getOrElse(Nil))
}

object FileSystem {
  val Null = new FileSystem {
    def scan(path: Path, offset: Option[Long], limit: Option[Long]): Process[Task, RenderedJson] = Process.halt

    def delete(path: Path): Task[Unit] = Task.now(())

    def ls(dir: Path): Task[Option[List[Path]]] = Task.now(if (dir == Path.Root) Some(Nil) else None)
  }
}