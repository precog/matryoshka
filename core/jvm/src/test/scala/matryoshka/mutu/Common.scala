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

import matryoshka.∘
import matryoshka.mutu.KEqual.ops._

import scala.Int

import scalaz._, Scalaz._

sealed trait Value[A[_], I]
final case class Lit[A[_]](i: Int) extends Value[A, Int]
final case class Pair[A[_], I, J](l: A[I], r: A[J]) extends Value[A, (I, J)]

object Value {
  implicit val htraverse: HTraverse[Value] = new HTraverse[Value] {
    def htraverse[F[_]: Applicative, A[_], B[_]](natM: A ~> (F ∘ B)#λ) =
      new (Value[A, ?] ~> (F ∘ Value[B, ?])#λ) {
        def apply[I](v: Value[A, I]) = v match {
          case Lit(i) => Applicative[F].point(Lit(i))
          case Pair(l, r) => (natM(l) ⊛ natM(r))(Pair(_, _))
        }
      }
  }

  implicit val equalhf: EqualHF[Value] = new EqualHF[Value] {
    def eqHF[G[_]: KEqual, I, J](l: Value[G, I], r: Value[G, J]) =
      (l, r) match {
        case (Lit(i1),      Lit(i2))      => i1 ≟ i2
        case (Pair(l1, r1), Pair(l2, r2)) => l1.keq(l2) && r1.keq(r2)
        case (_,            _)            => false
      }
  }
}

sealed trait Op[A[_], I]
final case class Add[A[_]](l: A[Int], r: A[Int]) extends Op[A, Int]
final case class Mult[A[_]](l: A[Int], r: A[Int]) extends Op[A, Int]
final case class Fst[A[_], I, J](p: A[(I, J)]) extends Op[A, I]
final case class Snd[A[_], I, J](p: A[(I, J)]) extends Op[A, J]

object Op {
  implicit val htraverse: HTraverse[Op] = new HTraverse[Op] {
    def htraverse[F[_]: Applicative, A[_], B[_]](natM: A ~> (F ∘ B)#λ) =
      new (Op[A, ?] ~> (F ∘ Op[B, ?])#λ) {
        def apply[I](v: Op[A, I]) = v match {
          case Add(l, r) => (natM(l) ⊛ natM(r))(Add(_, _))
          case Mult(l, r) => (natM(l) ⊛ natM(r))(Add(_, _))
          case Fst(p) => natM(p).map(Fst(_))
          case Snd(p) => natM(p).map(Snd(_))
        }
      }
  }

  implicit val equalhf: EqualHF[Op] = new EqualHF[Op] {
    def eqHF[G[_]: KEqual, I, J](l: Op[G, I], r: Op[G, J]) = (l, r) match {
      case (Add(l1, r1),  Add(l2, r2))  => l1.keq(l2) && r1.keq(r2)
      case (Mult(l1, r1), Mult(l2, r2)) => l1.keq(l2) && r1.keq(r2)
      case (Fst(p1),      Fst(p2))      => p1.keq(p2)
      case (Snd(p1),      Snd(p2))      => p1.keq(p2)
      case (_,            _)            => false
    }
  }
}
