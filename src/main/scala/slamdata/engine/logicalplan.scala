package slamdata.engine

sealed trait LogicalPlan

object LogicalPlan {
  /**
   * Reads data from the specified resources. The identity of each row of data 
   * is given by the identity projection, while the value of each row of data
   * is given by the value projection.
   */
  case class Read(resource: String, idProj: Lambda, valueProj: Lambda) extends LogicalPlan

  case class Constant(data: Data) extends LogicalPlan

  case class Filter(input: LogicalPlan, predicate: Lambda) extends LogicalPlan

  case class Join(left: LogicalPlan, right: LogicalPlan, leftProj: Lambda, rightProj: Lambda, joinType: JoinType) extends LogicalPlan

  case class Invoke(func: Func, values: List[LogicalPlan]) extends LogicalPlan

  case class Free(name: String) extends LogicalPlan

  case class Lambda(name: String, value: LogicalPlan) extends LogicalPlan

  case class Sort(value: LogicalPlan, by: Lambda) extends LogicalPlan

  case class Group(value: LogicalPlan, by: Lambda) extends LogicalPlan

  case class Take(value: LogicalPlan, count: Long) extends LogicalPlan

  case class Drop(value: LogicalPlan, count: Long) extends LogicalPlan

  sealed trait JoinType
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
}

