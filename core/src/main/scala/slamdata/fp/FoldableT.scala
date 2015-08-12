package slamdata.fp

import slamdata.Predef._

import scalaz._; import Scalaz._
import simulacrum.typeclass

/** Recursive operations over Foldable data structures. */
@typeclass trait FoldableT[T[_[_]]] {
  def all[F[_]: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).conjunction))

  def any[F[_]: Foldable](t: T[F])(p: T[F] ⇒ Boolean): Boolean =
    Tag.unwrap(foldMap(t)(p(_).disjunction))

  def contains[F[_]: EqualF: Foldable](t: T[F], c: T[F])(implicit T: Equal[T[F]]): Boolean =
    any(t)(_ ≟ c)

  def foldMap[F[_]: Foldable, Z: Monoid](t: T[F])(f: T[F] => Z): Z =
    foldMapM[F, Free.Trampoline, Z](t)(f(_).pure[Free.Trampoline]).run

  def foldMapM[F[_]: Foldable, M[_]: Monad, Z: Monoid](t: T[F])(f: T[F] => M[Z]): M[Z]

  def collect[F[_]: Foldable, B](t: T[F])(pf: PartialFunction[T[F], B]):
      List[B] =
    foldMap(t)(pf.lift(_).toList)
}
