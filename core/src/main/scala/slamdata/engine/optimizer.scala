package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._

import scalaz._
import Scalaz._

object Optimizer {
  import LogicalPlan._

  private def countUsage(target: Symbol): LogicalPlan[Int] => Int = {
    case FreeF(symbol) if symbol == target => 1
    case LetF(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inline[A](target: Symbol, repl: Term[LogicalPlan]):
      LogicalPlan[(Term[LogicalPlan], Term[LogicalPlan])] => Term[LogicalPlan] =
    {
      case FreeF(symbol) if symbol == target => repl
      case LetF(ident, form, body) if ident == target =>
        Let(ident, form._2, body._1)
      case x => Term(x.map(_._2))
    }

  val simplify: LogicalPlan[Term[LogicalPlan]] => Term[LogicalPlan] = {
    case LetF(ident, form, in) => in.cata(countUsage(ident)) match {
      case 0 => in
      case 1 => in.para(inline(ident, form))
      case _ => Let(ident, form, in)
    }
    case x => Term(x)
  }
}
