/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package matryoshka.patterns

import matryoshka._

import scalaz._, Scalaz._

/** This is the transformer for the (,) comonad.
  */
final case class EnvT[E, W[_], A](run: (E, W[A])) { self =>
  import EnvT._

  def runEnvT: (E, W[A]) = run

  def ask: E = run._1

  def lower: W[A] = run._2

  def map[B](f: A => B)(implicit W: Functor[W]): EnvT[E, W, B] =
    envT((run._1, run._2.map(f)))
}

object EnvT extends EnvTInstances with EnvTFunctions {
  def hmap[F[_], G[_], E, A](f: F ~> G) =
    λ[EnvT[E, F, ?] ~> EnvT[E, G, ?]](env => EnvT((env.ask, f(env.lower))))

  def htraverse[G[_]: Applicative, F[_], H[_], A](f: F ~> (G ∘ H)#λ) =
    λ[EnvT[A, F, ?] ~> (G ∘ EnvT[A, H, ?])#λ](_.run.traverse(f(_)).map(EnvT(_)))

  def lower[F[_], E] = λ[EnvT[E, F, ?] ~> F](_.lower)
}

sealed abstract class EnvTInstances1 {
  implicit def functor[E, W[_]](implicit W0: Functor[W]):
      Functor[EnvT[E, W, ?]] =
    new EnvTFunctor[E, W] {
      implicit def W: Functor[W] = W0
    }
}

sealed abstract class EnvTInstances0 extends EnvTInstances1 {
  implicit def cobind[E, W[_]](implicit W0: Cobind[W]):
      Cobind[EnvT[E, W, ?]] =
    new EnvTCobind[E, W] {
      implicit def W = W0
    }
}

sealed abstract class EnvTInstances extends EnvTInstances0 {
  implicit def comonad[E, W[_]](implicit W0: Comonad[W]): Comonad[EnvT[E, W, ?]] =
    new EnvTComonad[E, W] { implicit def W: Comonad[W] = W0 }

  implicit def equal[E: Equal, W[_]](implicit W: Delay[Equal, W]): Delay[Equal, EnvT[E, W, ?]] =
    new Delay[Equal, EnvT[E, W, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal((a, b) => a.ask ≟ b.ask && W(eq).equal(a.lower, b.lower))
    }

  implicit def show[E: Show, F[_]](implicit F: Delay[Show, F]): Delay[Show, EnvT[E, F, ?]] =
    new Delay[Show, EnvT[E, F, ?]] {
      def apply[A](sh: Show[A]) =
        Show.show(envt =>
          Cord("EnvT(") ++ envt.ask.show ++ Cord(", ") ++ F(sh).show(envt.lower) ++ Cord(")"))
    }

  implicit def bitraverse[F[_]](implicit F0: Traverse[F]): Bitraverse[EnvT[?, F, ?]] =
    new EnvTBitraverse[F] { implicit def F: Traverse[F] = F0 }

  implicit def traverse[E, F[_]: Traverse]: Traverse[EnvT[E, F, ?]] =
    bitraverse[F].rightTraverse
}

trait EnvTFunctions {
  def envT[E, W[_], A](v: (E, W[A])): EnvT[E, W, A] = EnvT(v)

  def cofreeIso[E, W[_]] = AlgebraIso[EnvT[E, W, ?], Cofree[W, E]](
    et => Cofree(et.ask, et.lower))(
    cof => EnvT((cof.head, cof.tail)))
}

//
// Type class implementation traits
//

private trait EnvTFunctor[E, W[_]] extends Functor[EnvT[E, W, ?]] {
  implicit def W: Functor[W]

  override final def map[A, B](fa: EnvT[E, W, A])(f: A => B) = fa map f
}

private trait EnvTBitraverse[F[_]]
    extends Bitraverse[EnvT[?, F, ?]]
    // with EnvTBiFunctor[F]
    // with EnvTBifoldable[F]
{
  implicit def F: Traverse[F]

  override final def bitraverseImpl[G[_]: Applicative, A, B, C, D]
    (fab: EnvT[A, F, B])
    (f: A ⇒ G[C], g: B ⇒ G[D]) =
    fab.run.bitraverse(f, _.traverse(g)) ∘ (EnvT(_))
}

private trait EnvTCobind[E, W[_]] extends Cobind[EnvT[E, W, ?]] with EnvTFunctor[E, W] {
  implicit def W: Cobind[W]

  override final def cojoin[A](fa: EnvT[E, W, A]): EnvT[E, W, EnvT[E, W, A]] =
    EnvT((fa.ask, fa.lower.cobind(x => EnvT((fa.ask, x)))))

  override final def cobind[A, B](fa: EnvT[E, W, A])(f: EnvT[E, W, A] => B): EnvT[E, W, B] =
    cojoin(fa).map(f)
}

private trait EnvTComonad[E, W[_]] extends Comonad[EnvT[E, W, ?]] with EnvTCobind[E, W] {
  implicit def W: Comonad[W]

  def copoint[A](p: EnvT[E, W, A]): A = p.lower.copoint
}
