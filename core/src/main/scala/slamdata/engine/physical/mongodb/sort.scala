package slamdata.engine.physical.mongodb

sealed trait SortType {
  def bson: Bson = this match {
    case Ascending => Bson.Int32(1)
    case Descending => Bson.Int32(-1)
  }
}
final case object Ascending extends SortType
final case object Descending extends SortType
