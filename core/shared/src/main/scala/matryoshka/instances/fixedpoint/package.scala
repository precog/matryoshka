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

package matryoshka.instances

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._

import monocle.Prism
import scala.{Boolean, Int, None, Option, Some}
import scala.annotation.{tailrec}

import scalaz._, Scalaz._

/** This package provides instances of various common data structures
  * implemented explicitly as fixed-points.
  */
package object fixedpoint {

  /** Natural numbers represented as the least fixed-point of [[scala.Option]].
    */
  type Nat = Mu[Option]

  object Nat {
    def fromInt: CoalgebraM[Option, Option, Int] =
      x => if (x < 0) None else Some(if (x > 0) (x - 1).some else None)

    // NB: This isn’t defined via `AlgebraPrism` because it only holds across a
    //     recursive structure.
    def intPrism[T](implicit TR: Recursive.Aux[T, Option], TC: Corecursive.Aux[T, Option]) =
      Prism[Int, T](_.anaM[T](fromInt))(_.cata(height))
  }

  implicit class NatOps[T]
    (self: T)
    (implicit TR: Recursive.Aux[T, Option], TC: Corecursive.Aux[T, Option]) {
    def +(other: T) = self.cata[T] {
      case None => other
      case o    => o.embed
    }

    def min(other: T) =
      (self, other).ana[T](_.bimap(_.project, _.project) match {
        case (None,    _)       => None
        case (_,       None)    => None
        case (Some(a), Some(b)) => Some((a, b))
    })

    def max(other: T) =
      (self, other).apo[T](_.bimap(_.project, _.project) match {
        case (None,    b)       => b ∘ (_.left)
        case (a,       None)    => a ∘ (_.left)
        case (Some(a), Some(b)) => Some((a, b).right)
      })
}

  /** The dual of [[Nat]], a potentially-infinite number. */
  type Conat = Nu[Option]
  object Conat {
    /** A representation of infinity, as a non-terminating corecursive process */
    val inf: Conat = ().ana[Nu[Option]](_.some)
  }

  type Free[F[_], A]   = Mu[CoEnv[A, F, ?]]
  type Cofree[F[_], A] = Mu[EnvT[A, F, ?]]
  type List[A]         = Mu[ListF[A, ?]]
  object List {
    def apply[A](elems: A*) =
      elems.ana[Mu[ListF[A, ?]]](ListF.seqIso[A].reverseGet)

    def tuple[A](elem: => A) = λ[Option ~> ListF[A, ?]] {
      case None    => NilF()
      case Some(b) => ConsF(elem, b)
    }

    def forget[A] = λ[ListF[A, ?] ~> Option] {
      case NilF()      => None
      case ConsF(_, t) => t.some
    }

    def fill[A](n: Nat)(elem: => A): List[A] = n.transAna(tuple(elem))
  }

  // FIXME: This implicit conversion seems to not get found, so we specialize
  //        `T` below.
  implicit class RecListFOps[T, A]
    (self: T)
    (implicit T: Recursive.Aux[T, ListF[A, ?]]) {
    def find(cond: A => Boolean): Option[A] = self.cata(ListF.find(cond))
    def length: Int = self.cata(size) - 1
    def headOption: Option[A] = self.project.headOption
    def tailOption: Option[T] = self.project.tailOption
    def take[N]
      (i: N)
      (implicit N: Recursive.Aux[N, Option], C: Corecursive.Aux[T, ListF[A, ?]])
        : T =
      (i, self).ana[T](ListF.takeUpTo)
  }

  implicit def ListOps[A](self: List[A]) = new RecListFOps[List[A], A](self)

  /** A lazy (potentially-infinite) list.
    */
  type Colist[A] = Nu[ListF[A, ?]]

  /** A true stream – infinite.
    */
  type Stream[A] = Nu[(A, ?)]

  object Stream {
    def matchesFirst[A, B](cond: A => Boolean) =
      λ[(A, ?) ~> (A \/ ?)] {
        case (h, t) => if (cond(h)) h.left else t.right
      }

    def take[N, T, A](implicit N: Recursive.Aux[N, Option], T: Recursive.Aux[T, (A, ?)]): Coalgebra[ListF[A, ?], (N, T)] = {
      case (n, s) =>
        n.project.fold[ListF[A, (N, T)]](
          NilF())(
          prev => {
            val pair = s.project
            ConsF(pair._1, (prev, pair._2))
          })
    }

    /** Colists are simply streams that may terminate, so a stream is easily
      * converted to a Colist that doesn’t terminate.
      */
    // TODO: This could be `toConsF` potentially.
    def toListF[A] = λ[(A, ?) ~> ListF[A, ?]](p => ConsF(p._1, p._2))
  }

  implicit class StreamOps[A](self: Nu[(A, ?)]) {
    def head: A = self.project._1

    def tail: Nu[(A, ?)] = self.project._2

    /** Drops exactly `n` elements from the stream.
      * This doesn’t expose the Coalgebra because it returns `Stream \/ Stream`,
      * which isn’t the type of `drop`.
      */
    def drop[N](n: N)(implicit N: Recursive.Aux[N, Option]): Nu[(A, ?)] =
      (n, self).anaM[Stream[A]] {
        case (r, stream) =>
          r.project.fold[Stream[A] \/ (A, (N, Stream[A]))](
            stream.left)(
            prev => stream.project.map((prev, _)).right)
      }.merge

    def take[N, T]
      (n: N)
      (implicit N: Recursive.Aux[N, Option], T: Corecursive.Aux[T, ListF[A, ?]])
        : T =
      (n, self).ana[T](Stream.take[N, Nu[(A, ?)], A])

    /** Colists are simply streams that may terminate, so a stream is easily
      * converted to a Colist that doesn’t terminate.
      */
    def toColist: Nu[ListF[A, ?]] = self.transAna(Stream.toListF(_))
  }

  /** Encodes a function that may diverge.
    */
  type Partial[A] = Nu[A \/ ?]

  object Partial {
    /** A partial function that immediately evaluates to the provided value.
      */
    def now[A](a: A): Partial[A] = a.left[Nu[A \/ ?]].embed

    def later[A](partial: Partial[A]): Partial[A] = partial.right[A].embed

    def delay[A](a: A): Option ~> (A \/ ?) = λ[Option ~> (A \/ ?)](_ \/> a)

    /** Canonical function that diverges.
      */
    def never[A]: Partial[A] = ().ana[Nu[A \/ ?]](_.right[A])

    /** This instance is not implicit, because it potentially runs forever.
      */
    def equal[A: Equal]: Equal[Partial[A]] =
      Equal.equal((a, b) => (a ≈ b).unsafePerformSync)

    def fromOption[A](opt: Option[A]): Partial[A] = opt.fold(never[A])(now)

    def fromPartialFunction[A, B](pf: scala.PartialFunction[A, B]):
        A => Partial[B] =
      pf.lift ⋙ fromOption
  }

  implicit val partialMonad: Monad[Partial] = new Monad[Partial] {
    def point[A](a: => A) = Partial.now(a)

    def bind[A, B](fa: Partial[A])(f: A => Partial[B]) =
      fa.project.fold(f, l => Partial.later(bind(l)(f)))
  }

  implicit class PartialOps[A](self: Nu[A \/ ?]) {
    def step: A \/ Partial[A] = self.project

    /** Returns `left` if the result was found within the given number of steps.
      */
    def runFor(steps: Int): A \/ Partial[A] =
      if (steps <= 0) step else step >>= (_.runFor(steps - 1))

    /** Run to completion (if it completes).
      */
    @tailrec final def unsafePerformSync: A = self.project match {
      case -\/(a) => a
      case \/-(p) => p.unsafePerformSync
    }

    // TODO: Would be nice to have this in ApplicativeOps
    def almostEqual[F[_]: Applicative, A: Equal](a: F[A], b: F[A]): F[Boolean] =
      (a ⊛ b)(_ ≟ _)

    /** If two `Partial`s eventually have the same value, then they are
      * equivalent.
      */
    def ≈(that: Partial[A])(implicit A: Equal[A]): Partial[Boolean] =
      almostEqual[Partial, A](self, that)
  }
}
