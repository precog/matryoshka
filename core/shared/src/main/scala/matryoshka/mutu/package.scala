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

package matryoshka

import scala.Unit

import scalaz._, Scalaz._

package object mutu {
  type GAlgebra[W[_[_], _], F[_[_], _], E[_]] = F[W[E, ?], ?] ~> E
  type Algebra[F[_[_], _], E[_]] = F[E, ?] ~> E
  type AlgebraM[M[_], F[_[_], _], E[_]] = F[E, ?] ~> (M ∘ E)#λ
  type GCoalgebra[M[_[_], _], F[_[_], _], A[_]] = A ~> F[M[A, ?], ?]
  type Coalgebra[F[_[_], _], A[_]] = A ~> F[A, ?]
  type CoalgebraM[M[_], F[_[_], _], A[_]] = A ~> (M ∘ F[A, ?])#λ

  type Context[F[_[_], _], A[_], I] = Ctx[HoleX, F, A, I]
  type TermX[F[_[_], _], I] = Ctx[NoHoleX, F, K[Unit, ?], I]
  type Const[F[_[_], _], I] = F[K[Unit, ?], I]

  implicit val termHRecursive: HRecursive[TermX] = new HRecursive[TermX] {
    def hproject[F[_[_], _], T](t: TermX[F, T]) = t match {
      case Term(t) => t
    }
  }

  final case class K[A, I](unK: A)

  def free[H, F[_[_], _]: HFunctor, A[_], B[_]](φ: Algebra[F, B], f: A ~> B):
      Ctx[H, F, A, ?] ~> B =
    new (Ctx[H, F, A, ?] ~> B) {
      // FIXME: Why is this triggering `asInstanceOf`?
      @java.lang.SuppressWarnings(scala.Array("org.brianmckenna.wartremover.warts.AsInstanceOf"))
      def apply[I](ctx: Ctx[H, F, A, I]) = ctx match {
        case Hole(v) => f(v)
        case Term(c) => φ(HFunctor[F].hmap(free[H, F, A, B](φ, f))(c))
      }
    }

  def cata0[H, F[_[_], _]: HFunctor, E[_]](φ: Algebra[F, E]):
      Ctx[H, F, E, ?] ~> E =
    free(φ, NaturalTransformation.refl)

  /** Lift a functor to a higher-level functor, much the way `Const` lifts a
    * proper type to a functor.
    */
  type HConst[F[_], G[_], I] = F[G[I]]

  trait :=>[F[_], A] {
    def apply[I](fi: F[I]): A

    type λ[I] = F[I] => A
  }

  abstract class :~>[F[_[_], _], G[_[_], _]] {
    def apply[A[_], I](fa: F[A, I]): G[A, I]
  }

  final case class FixH[F[_[_], _], A](hunFix: F[FixH[F, ?], A])
  object FixH {
    implicit val hrecursive: HRecursive[FixH] = new HRecursive[FixH] {
      def hproject[F[_[_], _], T](t: FixH[F, T]) = t.hunFix
    }

    implicit val hcorecursive: HCorecursive[FixH] = new HCorecursive[FixH] {
      def hembed[F[_[_], _], T](ft: F[FixH[F, ?], T]) = FixH(ft)
    }

    implicit def kequal[F[_[_], _]: EqualHF]: KEqual[FixH[F, ?]] = new KEqual[FixH[F, ?]] {
      def keq[I, J](l: FixH[F, I], r: FixH[F, J]) =
        EqualHF[F].eqHF(l.hunFix, r.hunFix)(kequal)
    }

    // implicit def equal[F[_[_], _]: EqualHF, I]: Equal[FixH[F,I]] =
    //   Equal.equal((a, b) => )
  }

  trait HRecursive[T[_[_[_], _], _]] {
    def hproject[F[_[_], _], A](t: T[F, A]): F[T[F, ?], A]

    def cata[F[_[_], _]: HFunctor, A[_]](φ: Algebra[F, A]): T[F, ?] ~> A =
      new (T[F, ?] ~> A) {
        def apply[Q](t: T[F, Q]) =
          φ(HFunctor[F].hmap(cata(φ))(hproject(t)))
      }

    def cataM[M[_]: Monad, F[_[_], _]: HTraverse, A[_]](φ: AlgebraM[M, F, A]):
        T[F, ?] ~> (M ∘ A)#λ =
      new (T[F, ?] ~> (M ∘ A)#λ) {
        def apply[I](t: T[F, I]) =
          HTraverse[F].htraverse(cataM(φ)).apply(hproject(t)) >>= (φ(_))
      }

    def para[F[_[_], _]: HFunctor, A[_]](
      φ: GAlgebra[λ[(γ[_], α) => (T[F, ?] :*: γ)#λ[α]], F, A]):
        T[F, ?] ~> A =
      new (T[F, ?] ~> A) {
        def apply[Q](t: T[F, Q]) =
          φ(HFunctor[F].hmap[T[F, ?], (T[F, ?] :*: A)#λ](
            new (T[F, ?] ~> (T[F, ?] :*: A)#λ) {
              def apply[P](t: T[F, P]) = (t, para(φ).apply(t))
            })(hproject[F, Q](t)))
      }

  }
  object HRecursive {
    def apply[T[_[_[_], _], _]](implicit T: HRecursive[T]) = T
  }

  trait HCorecursive[T[_[_[_], _], _]] {
    def hembed[F[_[_], _], A](ft: F[T[F, ?], A]): T[F, A]

    def ana[F[_[_], _]: HFunctor, A[_]](ψ: Coalgebra[F, A]): A ~> T[F, ?] =
      new (A ~> T[F, ?]) {
        def apply[I](a: A[I]) = hembed(HFunctor[F].hmap(ana(ψ))(ψ(a)))
      }

    def anaM[M[_]: Monad, F[_[_], _]: HTraverse, A[_]](ψ: CoalgebraM[M, F, A]):
        A ~> (M ∘ T[F, ?])#λ =
      new (A ~> (M ∘ T[F, ?])#λ) {
        def apply[I](a: A[I]) =
          (ψ(a) >>= (HTraverse[F].htraverse(anaM(ψ)).apply(_))) ∘ (hembed(_))
      }
  }
  object HCorecursive {
    def apply[T[_[_[_], _], _]](implicit T: HCorecursive[T]) = T
  }

  sealed trait HCoproduct[F[_[_], _], G[_[_], _], H[_], E]
  final case class Inl[F[_[_], _], G[_[_], _], H[_], E](out: F[H, E])
      extends HCoproduct[F, G, H, E]
  final case class Inr[F[_[_], _], G[_[_], _], H[_], E](out: G[H, E])
      extends HCoproduct[F, G, H, E]

  object HCoproduct {
    implicit def equalHF[F[_[_], _]: EqualHF, G[_[_], _]: EqualHF]:
        EqualHF[(F ^+^ G)#λ] =
      new EqualHF[(F ^+^ G)#λ] {
        def eqHF[H[_]: KEqual, I, J](l: (F ^+^ G)#λ[H, I], r: (F ^+^ G)#λ[H, J]) =
          (l, r) match {
            case (Inl(x), Inl(y)) => EqualHF[F].eqHF(x, y)
            case (Inr(x), Inr(y)) => EqualHF[G].eqHF(x, y)
            case (_,      _)      => false
          }
      }

    implicit def hFunctor[F[_[_], _]: HFunctor, G[_[_], _]: HFunctor]:
        HFunctor[(F ^+^ G)#λ] =
      new HFunctor[(F ^+^ G)#λ] {
        def hmap[Q[_], P[_]](f: Q ~> P): (F ^+^ G)#λ[Q, ?] ~> (F ^+^ G)#λ[P, ?] =
          new ((F ^+^ G)#λ[Q, ?] ~> (F ^+^ G)#λ[P, ?]) {
            def apply[A](in: (F ^+^ G)#λ[Q, A]) = in match {
              case Inl(v) => Inl(HFunctor[F].hmap(f)(v))
              case Inr(v) => Inr(HFunctor[G].hmap(f)(v))
            }
          }
      }
  }

  trait :*:[F[_], G[_]] {
    type λ[A] = (F[A], G[A])
  }

  trait ^+^[F[_[_], _], G[_[_], _]] {
    type λ[H[_], E] = HCoproduct[F, G, H, E]
  }

  type ^<^[F[_[_], _], G[_[_], _]] = HInject[F, G]
}
