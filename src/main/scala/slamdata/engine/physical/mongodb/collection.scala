package slamdata.engine.physical.mongodb

case class Collection(name: String) {
  override def toString = s"""Collection("$name")"""
}