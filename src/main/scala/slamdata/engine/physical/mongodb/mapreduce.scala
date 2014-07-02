package slamdata.engine.physical.mongodb

case class MapReduce(
  map:        Js.FunDecl, 
  reduce:     Js.FunDecl, 
  selection:  Option[FindQuery] = None, 
  inputSort:  Option[Map[String, SortType]] = None, 
  limit:      Option[Int] = None,
  finalizer:  Option[Js.FunDecl] = None, 
  scope:      Option[Map[String, Bson]] = None, 
  jsMode:     Option[Boolean] = None,
  verbose:    Option[Boolean] = None
)
