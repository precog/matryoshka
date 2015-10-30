package quasar

import quasar.Predef._

import simulacrum.typeclass

import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.functor._

/** A source of strings unique within `F[_]`, an implementation must have the
  * property that, if Applicative[F], then (freshName |@| freshName)(_ != _).
  */
@typeclass
trait NameGenerator[F[_]] {
  /** Returns a fresh name, guaranteed to be unique among all the other names
    * generated from `F`.
    */
  def freshName: F[String]

  /** Returns a fresh name, prefixed with the given string. */
  def prefixedName(prefix: String)(implicit F: Functor[F]): F[String] =
    freshName map (prefix + _)
}

object NameGenerator {

  /** A short, randomized string to use as "salt" in salted name generators. */
  val salt: Task[String] =
    Task.delay(scala.util.Random.nextInt().toHexString)

  implicit def sequenceNameGenerator[F[_]: Monad]: NameGenerator[SeqNameGeneratorT[F, ?]] =
    new NameGenerator[SeqNameGeneratorT[F, ?]] {
      val ms = MonadState[StateT[F, ?, ?], Long]
      def freshName = ms.get flatMap (n => ms.put(n + 1) as n.toString)
    }

  implicit def saltedSequenceNameGenerator[F[_]: Monad]: NameGenerator[SaltedSeqNameGeneratorT[F, ?]] =
    new NameGenerator[SaltedSeqNameGeneratorT[F, ?]] {
      type G[A] = SeqNameGeneratorT[F, A]
      def freshName = ReaderT[G, String, String] { salt: String =>
        NameGenerator[G].freshName.map(s"${salt}_" + _)
      }
    }

  implicit def eitherTNameGenerator[F[_]: NameGenerator : Functor, E]: NameGenerator[EitherT[F, E, ?]] =
    new NameGenerator[EitherT[F, E, ?]] {
      def freshName = EitherT.right(NameGenerator[F].freshName)
    }
}

