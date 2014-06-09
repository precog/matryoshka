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
  def scan(path: Path): Process[Task, RenderedJson]

  def delete(path: Path): Task[Unit]

  def ls: Task[List[Path]]
}

object FileSystem {
  val Null = new FileSystem {
    def scan(path: Path): Process[Task, RenderedJson] = Process.halt

    def delete(path: Path): Task[Unit] = Task.now(())

    def ls: Task[List[Path]] = Task.now(Nil)
  }
}