package slamdata.engine.physical.mongodb

case class MapReduce(
  map:        String, 
  reduce:     String, 
  selection:  Option[Query] = None, 
  inputSort:  Option[Map[String, SortType]] = None, 
  limit:      Option[Int] = None,
  finalizer:  Option[String] = None, 
  scope:      Option[Map[String, Bson]] = None, 
  jsMode:     Option[Boolean] = None,
  verbose:    Option[Boolean] = None
)