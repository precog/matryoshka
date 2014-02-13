package slamdata.engine.physical.mongodb

sealed trait SortType
case object Ascending extends SortType
case object Descending extends SortType