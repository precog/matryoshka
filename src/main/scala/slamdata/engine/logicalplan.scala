package slamdata.engine

sealed trait LogicalPlan

object LogicalPlan {
  case class Pure(data: Data) extends LogicalPlan

  case class Filter(input: LogicalPlan, predicate: Lambda) extends LogicalPlan

  case class Join(left: LogicalPlan, right: LogicalPlan, leftProj: Lambda, rightProj: Lambda, joinType: JoinType) extends LogicalPlan

  case class Invoke(values: Seq[LogicalPlan], func: Func) extends LogicalPlan

  case class Free(name: String) extends LogicalPlan

  case class Lambda(name: String, value: LogicalPlan) extends LogicalPlan

  case class Sort(value: LogicalPlan, by: Lambda) extends LogicalPlan

  case class Group(value: LogicalPlan, by: Lambda) extends LogicalPlan

  case class ObjectConcat(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan

  case class ArrayConcat(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan

  case class MakeObject(fields: Map[String, LogicalPlan]) extends LogicalPlan

  case class MakeArray(elements: Seq[LogicalPlan]) extends LogicalPlan

  case class ObjectProject(obj: LogicalPlan, name: String) extends LogicalPlan

  case class ArrayProject(arr: LogicalPlan, index: Int) extends LogicalPlan

  sealed trait JoinType
  case object Inner extends JoinType
  case object LeftOuter extends JoinType
  case object RightOuter extends JoinType
  case object FullOuter extends JoinType
}

