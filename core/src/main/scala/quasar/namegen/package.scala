package quasar

import quasar.Predef._

import scalaz._, Scalaz._

package object namegen {

  final case class NameGen(nameGen: Int)

  implicit val NameGenMonoid: Monoid[NameGen] = new Monoid[NameGen] {
    def zero = NameGen(0)
    def append(f1: NameGen, f2: => NameGen) = NameGen(f1.nameGen max f2.nameGen)
  }

  def freshName(label: String): State[NameGen, String] = for {
    n <- State((s: NameGen) => s.copy(nameGen = s.nameGen + 1) -> s.nameGen)
  } yield "__" + label + n.toString

  type NameT[M[_], A] = StateT[M, NameGen, A]
  type NameDisj[E, A] = NameT[E \/ ?, A]

  class LiftHelper[F[_]] {
    def apply[A](v: F[A])(implicit F: Functor[F]) =
      StateT[F, NameGen, A](s => F.map(v)(s -> _))
  }

  def lift[F[_]] = new LiftHelper[F]

  def emit[F[_]: Applicative, A](v: A): NameT[F, A] =
    lift(v.point[F])
  def emitName[F[_]: Applicative, A](v: State[NameGen, A]): NameT[F, A] =
    StateT[F, NameGen, A](s => v.run(s).point[F])
}
