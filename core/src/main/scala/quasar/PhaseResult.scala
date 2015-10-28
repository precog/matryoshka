package quasar

import quasar.Predef._

import argonaut._, Argonaut._
import scalaz.Show

sealed trait PhaseResult {
  def name: String
}

object PhaseResult {
  final case class Tree(name: String, value: RenderedTree) extends PhaseResult {
    override def toString = name + "\n" + Show[RenderedTree].shows(value)
  }
  final case class Detail(name: String, value: String) extends PhaseResult {
    override def toString = name + "\n" + value
  }

  implicit def phaseResultEncodeJson: EncodeJson[PhaseResult] = EncodeJson {
    case Tree(name, value)   => Json.obj("name" := name, "tree" := value)
    case Detail(name, value) => Json.obj("name" := name, "detail" := value)
  }
}
