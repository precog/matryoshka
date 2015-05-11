package slamdata.engine.fp

import scalaz._
import Scalaz._

/**
 * This is the transformer for the (,) comonad.
 */
final case class EnvT[E, W[_], A](run: (E, W[A])) { self =>
  import EnvT._

  def runEnvT: (E, W[A]) = run

  def ask: E = run._1

  def lower: W[A] = run._2

  def map[B](f: A => B)(implicit W: Functor[W]): EnvT[E, W, B] =
    envT((run._1, run._2.map(f)))
}

object EnvT extends EnvTInstances with EnvTFunctions

sealed abstract class EnvTInstances0 {
  implicit def envTFunctor[E, W[_]](implicit W0: Functor[W]): Functor[EnvT[E, W, ?]] = new EnvTFunctor[E, W] {
    implicit def W = W0
  }
}

sealed abstract class EnvTInstances extends EnvTInstances0 {
  implicit def envTComonad[E, W[_]]: Comonad[EnvT[E, W, ?]] = new EnvTComonad[E, W] {
    implicit def W = implicitly
  }
}

trait EnvTFunctions {
  def envT[E, W[_], A](v: (E, W[A])): EnvT[E, W, A] = EnvT(v)
}

//
// Type class implementation traits
//

private trait EnvTFunctor[E, W[_]] extends Functor[EnvT[E, W, ?]] {
  implicit def W: Functor[W]

  override def map[A, B](fa: EnvT[E, W, A])(f: A => B) = fa map f
}

private trait EnvTComonad[E, W[_]] extends Comonad[EnvT[E, W, ?]] with EnvTFunctor[E, W] {
  implicit def W: Comonad[W]

  override def cojoin[A](fa: EnvT[E, W, A]): EnvT[E, W, EnvT[E, W, A]] =
    EnvT((fa.ask, fa.lower.cobind(x => EnvT((fa.ask, x)))))

  def cobind[A, B](fa: EnvT[E, W, A])(f: EnvT[E, W, A] => B): EnvT[E, W, B] =
    cojoin(fa).map(f)

  def copoint[A](p: EnvT[E, W, A]): A = p.lower.copoint

}
