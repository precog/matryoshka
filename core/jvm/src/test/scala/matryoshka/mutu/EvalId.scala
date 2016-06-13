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

import org.specs2.mutable._
import scalaz._, Scalaz._

trait EvalId[F[_[_], _]] {
  def evalId: Algebra[F, Id]
}
object EvalId {
  def apply[F[_[_], _]](implicit E: EvalId[F]) = E

  implicit def sum[F[_[_], _], G[_[_], _]](implicit F: EvalId[F], G: EvalId[G]):
      EvalId[(F ^+^ G)#λ] =
    new EvalId[(F ^+^ G)#λ] {
      def evalId =
        new Algebra[(F ^+^ G)#λ, Id] {
          def apply[I](fa: (F ^+^ G)#λ[Id, I]) = fa match {
            case Inl(f) => F.evalId(f)
            case Inr(g) => G.evalId(g)
          }
        }
    }

  implicit val value: EvalId[Value] = new EvalId[Value] {
    def evalId =
      new Algebra[Value, Id] {
        def apply[I](fa: Value[Id, I]) = fa match {
          case Lit(n)     => n
          case Pair(x, y) => (x, y)
        }
      }
  }

  implicit val op: EvalId[Op] = new EvalId[Op] {
    def evalId =
      new Algebra[Op, Id] {
        def apply[I](fa: Op[Id, I]) = fa match {
          case Add(x, y)   => x + y
          case Mult(x, y)  => x * y
          case Fst((x, _)) => x
          case Snd((_, y)) => y
        }
      }
  }
}

class EvalIdSpec extends Specification {
  type Sig[H[_], E] = (Op ^+^ Value)#λ[H, E]

  implicit val V = scala.Predef.implicitly[Value ^<^ Sig]
  implicit val O = scala.Predef.implicitly[Op ^<^ Sig]

  "evalId" should {
    "result in an integer" in {
      HRecursive[FixH].cata(EvalId[Sig].evalId).apply(FixH(O.inj(Fst(FixH(V.inj(Pair(FixH(V.inj(Lit[FixH[Sig, ?]](2))), FixH(V.inj(Lit[FixH[Sig, ?]](1)))))))))) must_== 2
    }
  }
}
