package quasar

import quasar.Predef._

import simulacrum._

import scalaz._
import scalaz.syntax.functor._

/** A source of strings unique within `F[_]`, an implementation must have the
  * property that, if Applicative[F], then (fresh |@| fresh)(_ != _).
  */
@typeclass
trait NameGenerator[F[_]] {
  def freshName: F[String]

  def prefixedName(prefix: String)(implicit F: Functor[F]): F[String] =
    freshName map (prefix + _)
}

object NameGenerator {
  implicit def sequenceNameGenerator[F[_]: Monad]: NameGenerator[SeqNameGeneratorT[F, ?]] =
    new NameGenerator[SeqNameGeneratorT[F, ?]] {
      val ms = MonadState[StateT[F, ?, ?], Long]
      def freshName = ms.get flatMap (n => ms.put(n + 1) as n.toString)
    }
}

