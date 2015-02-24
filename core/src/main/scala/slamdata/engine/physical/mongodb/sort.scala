package slamdata.engine.physical.mongodb

sealed trait SortType {
  def bson: Bson = this match {
    case Ascending => Bson.Int32(1)
    case Descending => Bson.Int32(-1)
  }
}
case object Ascending extends SortType
case object Descending extends SortType
