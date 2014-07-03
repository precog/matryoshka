package slamdata.engine.physical.mongodb

case class MapReduce(
  map:        Js.Expr,
  reduce:     Js.Expr,
  out:        Option[Output] = None,
  selection:  Option[FindQuery] = None,
  inputSort:  Option[Map[String, SortType]] = None,
  limit:      Option[Int] = None,
  finalizer:  Option[Js.FunDecl] = None,
  scope:      Option[Map[String, Bson]] = None,
  jsMode:     Option[Boolean] = None,
  verbose:    Boolean = false)

sealed trait Action

object Action {
  case object Replace extends Action
  case object Merge extends Action
  case object Reduce extends Action
}

case class Output(
  action:     Action = Action.Replace,
  sharded:    Option[Boolean] = None,
  nonAtomic:  Option[Boolean] = None)
