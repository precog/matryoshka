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

package matryoshka.mutu

import scala.{Int, None, Some}

import org.specs2.mutable._

trait Eval[F[_[_], _], V[_[_], _]] {
  def eval[T[_[_[_], _], _]: HRecursive: HCorecursive]: Algebra[F, T[V, ?]]
}

object Eval {
  def apply[F[_[_], _], V[_[_], _]](implicit E: Eval[F, V]) = E

  implicit def sum[F[_[_], _], G[_[_], _], V[_[_], _]](
    implicit F: Eval[F, V], G: Eval[G, V]):
      Eval[(F ^+^ G)#λ, V] =
    new Eval[(F ^+^ G)#λ, V] {
      def eval[T[_[_[_], _], _]: HRecursive: HCorecursive] =
        new Algebra[(F ^+^ G)#λ, T[V, ?]] {
          def apply[I](fa: (F ^+^ G)#λ[T[V, ?], I]) = fa match {
            case Inl(f) => F.eval[T].apply(f)
            case Inr(g) => G.eval[T].apply(g)
          }
        }
    }

  implicit def component[F[_[_], _], V[_[_], _]](implicit F: F ^<^ V):
      Eval[F, V] =
    new Eval[F, V] {
      def eval[T[_[_[_], _], _]: HRecursive: HCorecursive] =
        new Algebra[F, T[V, ?]] {
          def apply[I](fa: F[T[V, ?], I]) = HCorecursive[T].hembed(F.inj(fa))
      }
    }

  implicit def op[V[_[_], _]](implicit V: Value ^<^ V): Eval[Op, V] =
    new Eval[Op, V] {
      def eval[T[_[_[_], _], _]: HRecursive: HCorecursive] =
        new Algebra[Op, T[V, ?]] {
          def apply[I](fa: Op[T[V, ?], I]) = fa match {
            case Add(x, y) =>
              HCorecursive[T].hembed(V.inj(Lit(projC(x) + projC(y))))
            case Mult(x, y) =>
              HCorecursive[T].hembed(V.inj(Lit(projC(x) * projC(y))))
            case Fst(x) => projP(x)._1
            case Snd(x) => projP(x)._2
          }
        }
    }

  def projC[T[_[_[_], _], _]: HRecursive, V[_[_], _]](t: T[V, Int])(implicit V: Value ^<^ V): Int =
    V.prj(HRecursive[T].hproject(t)) match {
      case Some(Lit(n)) => n
      case None => scala.sys.error("no way, but gotta fix the types, I guess")
    }

  // TODO: Show that `None` is impossible
  def projP[T[_[_[_], _], _]: HRecursive, V[_[_], _], I, J](t: T[V, (I, J)])(implicit V: Value ^<^ V): (T[V, I], T[V, J]) =
    V.prj(HRecursive[T].hproject(t)) match {
      case Some(Pair(x, y)) => (x, y)
      case None => scala.sys.error("no way, but gotta fix the types, I guess")
    }
}

class EvalSpec extends Specification {
  type Sig[H[_], E] = (Op ^+^ Value)#λ[H, E]

  implicit val V = scala.Predef.implicitly[Value ^<^ Sig]
  implicit val O = scala.Predef.implicitly[Op ^<^ Sig]

  "eval" should {
    "result in a Value term" in {
      HRecursive[FixH].cata(Eval[Sig, Value].eval[FixH]).apply(FixH(O.inj(Add(FixH(V.inj(Lit[FixH[Sig, ?]](1))), FixH(O.inj(Mult(FixH(V.inj(Lit[FixH[Sig, ?]](2))), FixH(V.inj(Lit[FixH[Sig, ?]](2)))))))))) must_== FixH(Lit[FixH[Value, ?]](5))
    }

    "same" in {
      HRecursive[FixH].cata(Eval[Sig, Value].eval[FixH]).apply(FixH(O.inj(Fst(FixH(V.inj(Pair(FixH(V.inj(Lit[FixH[Sig, ?]](2))), FixH(V.inj(Lit[FixH[Sig, ?]](1)))))))))) must_== FixH(Lit[FixH[Value, ?]](2))
    }
  }
}
