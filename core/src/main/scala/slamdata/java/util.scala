package slamdata.java

object JavaUtil {
  def abbrev(trace: Array[StackTraceElement]): String = {
    TraceSegment.collapse(trace.toList.map { te =>
       if (te.getClassName.startsWith("slamdata")) SavedFrame(te)
       else DroppedSegment(te, 1, te)
    }).mkString("\n")
  }

  sealed trait TraceSegment
  case class SavedFrame(elem: StackTraceElement) extends TraceSegment {
    override def toString = elem.toString
  }
  case class DroppedSegment(first: StackTraceElement, count: Int, last: StackTraceElement) extends TraceSegment {
    override def toString = s"  ... ($count)"
  }
  object TraceSegment {
    def collapse(l: List[TraceSegment]): List[TraceSegment] = l match {
      case DroppedSegment(f1, c1, l1) :: DroppedSegment(f2, c2, l2) :: t =>
        collapse(DroppedSegment(f1, c1+c2, l2) :: t)
      case h :: t => h :: collapse(t)
      case Nil    => Nil
    }
  }
}
