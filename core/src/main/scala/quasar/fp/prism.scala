package quasar.fp

import monocle.Prism
import scalaz._

trait PrismInstances {
  import Liskov.<~<

  // TODO: See if we can implement this once for all tuples using shapeless.
  implicit class PrismOps[A, B](prism: Prism[A, B]) {
    def apply(b: B): A = prism.reverseGet(b)

    def apply[C, D](c: C, d: D)(implicit ev: (C, D) <~< B): A =
      apply(ev((c, d)))

    def apply[C, D, E](c: C, d: D, e: E)(implicit ev: (C, D, E) <~< B): A =
      apply(ev((c, d, e)))

    def apply[C, D, E, F](c: C, d: D, e: E, f: F)(implicit ev: (C, D, E, F) <~< B): A =
      apply(ev((c, d, e, f)))
  }
}

object prism extends PrismInstances
