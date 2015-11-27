package quasar

import java.lang.RuntimeException

import scalaz._
import scalaz.syntax.show._
import scalaz.syntax.monad._

object rethrow {
  /** "Throw" the lhs of an `EitherT` given a `Catchable` base type and the
    * ability to represent `E` as a `String`.
    */
  def apply[F[_]: Catchable : Monad, E: Show]: EitherT[F, E, ?] ~> F =
    new (EitherT[F, E, ?] ~> F) {
      def apply[A](et: EitherT[F, E, A]) =
        et.fold(
          e => Catchable[F].fail[A](new RuntimeException(e.shows)),
          _.point[F]
        ).join
    }
}
