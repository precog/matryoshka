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

  case class Filter(input: LogicalPlan, predicate: LogicalPlan) extends LogicalPlan

  case class Join(left: LogicalPlan, right: LogicalPlan, joinType: JoinType, joinRel: JoinRel, leftProj: Lambda, rightProj: Lambda) extends LogicalPlan

  case class Cross(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan

  case class Invoke(func: Func, values: List[LogicalPlan]) extends LogicalPlan

  case class Cond(pred: LogicalPlan, ifTrue: LogicalPlan, ifFalse: LogicalPlan) extends LogicalPlan

  case class Free(name: String) extends LogicalPlan

  case class Lambda(name: String, value: LogicalPlan) extends LogicalPlan

  case class Sort(value: LogicalPlan, by: LogicalPlan) extends LogicalPlan

  case class Group(value: LogicalPlan, by: LogicalPlan) extends LogicalPlan

  case class Take(value: LogicalPlan, count: Long) extends LogicalPlan

  case class Drop(value: LogicalPlan, count: Long) extends LogicalPlan

  sealed trait JoinType
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType

  sealed trait JoinRel
  object JoinRel {
    case object Eq extends JoinRel
    case object Neq extends JoinRel
    case object Lt extends JoinRel
    case object Lte extends JoinRel
    case object Gt extends JoinRel
    case object Gte extends JoinRel
  }
}

