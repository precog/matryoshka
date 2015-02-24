package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._

object Optimizer {

  def simplify(term: Term[LogicalPlan]): Term[LogicalPlan] = {
    def countUsage(start: Term[LogicalPlan], target: Symbol): Int = {
      (scanCata(attrUnit(start)) { (_ : Unit, lp: LogicalPlan[Int]) =>
        lp.fold(
          read      = κ(0),
          constant  = κ(0),
          join      = (l, r, _, _, lp, rp) => l + r + lp + rp,
          invoke    = (_, args) => args.sum,
          free      = symbol => if (symbol == target) 1 else 0,
          let       = (_, form, in) => form + in
        )
      }).unFix.attr
    }

    def inline(start: Term[LogicalPlan], target: Symbol, repl: Term[LogicalPlan]): Term[LogicalPlan] = {
      start.topDownTransform { (term: Term[LogicalPlan]) =>
        term.unFix.fold(
          read      = κ(term),
          constant  = κ(term),
          join      = κ(term),
          invoke    = κ(term),
          free      = symbol => if (symbol == target) repl else term,
          let       = κ(term)
        )
      }
    }

    term.topDownTransform { (term: Term[LogicalPlan]) =>
      def pass(term: Term[LogicalPlan]): Term[LogicalPlan] = {
        term.unFix.fold(
          read      = κ(term),
          constant  = κ(term),
          join      = κ(term),
          invoke    = κ(term),
          free      = κ(term),
          let       = (ident, form, in) => {
            if (countUsage(in, ident) <= 1)
              pass(inline(in, ident, form))
            else
              LogicalPlan.Let(ident, form, in)
          }
        )
      }

      pass(term)
    }
  }
}
