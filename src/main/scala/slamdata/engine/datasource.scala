package slamdata.engine

import scalaz.\/
import scalaz.concurrent._
import scalaz.stream._

import argonaut._

case class RenderedJson(value: String) {
  def toJson: String \/ Json = JsonParser.parse(value)

  override def toString = value
}

trait DataSource {
  def scan(table: String): Process[Task, RenderedJson]

  def delete(table: String): Task[Unit]
}