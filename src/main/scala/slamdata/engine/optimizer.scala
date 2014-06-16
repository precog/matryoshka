package slamdata.engine

import slamdata.engine.analysis.fixplate._

object Optimizer {

  def simplify(term: Term[LogicalPlan]): Term[LogicalPlan] = {
    def countUsage(start: Term[LogicalPlan], target: Symbol): Int = {
      (scanCata(attrUnit(start)) { (_ : Unit, lp: LogicalPlan[Int]) => 
        lp.fold(
          read      = _ => 0,
          constant  = _ => 0,
          join      = (l, r, _, _, lp, rp) => l + r + lp + rp,
          invoke    = (_, args) => args.sum,
          free      = symbol => if (symbol == target) 1 else 0,
          let       = (let, in) => let.values.sum + in
        )
      }).unFix.attr
    }

    def inline(start: Term[LogicalPlan], target: Symbol, repl: Term[LogicalPlan]): Term[LogicalPlan] = {
      start.topDownTransform { (term: Term[LogicalPlan]) =>
        term.unFix.fold(
          read      = _ => term,
          constant  = _ => term,
          join      = (_, _, _, _, _, _) => term,
          invoke    = (_, _) => term,
          free      = symbol => if (symbol == target) repl else term,
          let       = (let, in) => term
        )
      }
    }

    term.topDownTransform { (term: Term[LogicalPlan]) =>
      def pass(term: Term[LogicalPlan]): Term[LogicalPlan] = {
        term.unFix.fold(
          read      = _ => term,
          constant  = _ => term,
          join      = (l, r, _, _, lp, rp) => term,
          invoke    = (_, args) => term,
          free      = symbol => term,
          let       = (let, in) => {
                        val (inlined, in2) = let.foldLeft((Nil: List[Symbol], in)) {
                          case ((inlined, in), (symbol, term)) =>
                            if (countUsage(in, symbol) <= 1) (symbol :: inlined) -> inline(in, symbol, term)
                            else inlined -> in
                        }

                        val remainder = let -- inlined

                        (if (remainder.size == 0) pass(in2) else LogicalPlan.let(remainder, in2))
                      }
        )
      }

      pass(term)
    }
  }
}