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
  def scan(table: String): Process[Task, RenderedJson]

  def delete(table: String): Task[Unit]

  def ls: Task[List[String]]
}

object FileSystem {
  val Null = new FileSystem {
    def scan(table: String): Process[Task, RenderedJson] = Process.halt

    def delete(table: String): Task[Unit] = Task.now(())

    def ls: Task[List[String]] = Task.now(Nil)
  }
}