/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.stacktrace

import quasar.Predef._
import java.lang.StackTraceElement

object StackUtil {
  def abbrev(trace: Array[StackTraceElement]): String = {
    TraceSegment.collapse(trace.toList.map { te =>
       if (te.getClassName.startsWith("quasar")) SavedFrame(te)
       else DroppedSegment(te, 1, te)
    }).mkString("\n")
  }

  sealed trait TraceSegment
  final case class SavedFrame(elem: StackTraceElement) extends TraceSegment {
    override def toString = elem.toString
  }
  final case class DroppedSegment(first: StackTraceElement, count: Int, last: StackTraceElement) extends TraceSegment {
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
