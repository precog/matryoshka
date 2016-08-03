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

import matryoshka._, Recursive.ops._
import matryoshka.patterns._

import monocle.Prism
import scala.{Boolean, Int, None, Option, Some}
import scala.annotation.{tailrec}

import scalaz._, Scalaz._

/** This package provides instances of various common data structures
  * implemented explicitly as fixed-points.
  */
package object fixedpoint {
  type Nat = Mu[Option]
  object Nat {
    def fromInt: CoalgebraM[Option, Option, Int] =
      x => if (x < 0) None else Some(if (x > 0) (x - 1).some else None)

    // NB: This isn’t defined via `AlgebraPrism` because it only holds across a
    //     recursive structure.
    def intPrism = Prism[Int, Fix[Option]](_.anaM(fromInt))(_.cata(height))
  }

  implicit class NatOps[T[_[_]]: Recursive: Corecursive](self: T[Option]) {
    def +(other: T[Option]) = self.cata[T[Option]] {
      case None => other
      case o    => o.embed
    }

    def min(other: T[Option]) =
      (self, other).ana(_.bimap(_.project, _.project) match {
        case (None,    _)       => None
        case (_,       None)    => None
        case (Some(a), Some(b)) => Some((a, b))
    })

    def max(other: T[Option]) =
      (self, other).apo(_.bimap(_.project, _.project) match {
        case (None,    b)       => b ∘ (_.left)
        case (a,       None)    => a ∘ (_.left)
        case (Some(a), Some(b)) => Some((a, b).right)
    })
}

  type Conat = Nu[Option]
  object Conat {
    val inf: Conat = ().ana[Nu, Option](_.some)
  }

  type Free[F[_], A]   = Mu[CoEnv[A, F, ?]]
  type Cofree[F[_], A] = Mu[EnvT[A, F, ?]]
  type List[A]         = Mu[ListF[A, ?]]
  object List {
    def apply[A](elems: A*) =
      elems.ana[Mu, ListF[A, ?]](ListF.seqIso[A].reverseGet)

    def fillƒ[A](elem: => A): Option ~> ListF[A, ?] =
      new (Option ~> ListF[A, ?]) {
        def apply[β](opt: Option[β]) = opt match {
          case None    => NilF()
          case Some(b) => ConsF(elem, b)
        }
      }

    def fill[A](n: Int)(elem: => A): List[A] =
      n.hyloM(
        transformToAlgebra[Mu, Id, Option, Option, ListF[A, ?]](fillƒ(elem).apply(_).point[Option]),
        Nat.fromInt)
        .getOrElse(NilF[A, List[A]]().embed)
  }

  implicit class ListOps[A](self: Mu[ListF[A, ?]]) {
    def length: Int = self.cata(size) - 1
  }

  implicit class RecListFOps[T[_[_]]: Recursive, A](self: T[ListF[A, ?]]) {
    def headOption: Option[A]              = self.project.headOption
    def tailOption: Option[T[ListF[A, ?]]] = self.project.tailOption
  }

  /** A lazy (potentially-infinite) list.
    */
  type Colist[A] = Nu[ListF[A, ?]]

  /** A true stream – infinite.
    */
  type Stream[A] = Nu[(A, ?)]

  implicit class StreamOps[A](self: Nu[(A, ?)]) {
    def head: A = self.project._1
    def tail: Nu[(A, ?)] = self.project._2
    @tailrec final def drop(n: Int): Nu[(A, ?)] =
      if (n > 0) tail.drop(n - 1) else self
    def take(n: Int): Mu[ListF[A, ?]] =
      (n, self).ana[Mu, ListF[A, ?]] {
        case (r, stream) if (r > 0) => ConsF(stream.head, (r - 1, stream.tail))
        case (_, _)                 => NilF()
      }
  }

  /** Encodes a function that may diverge.
    */
  type Partial[A] = Nu[A \/ ?]

  object Partial {
    def now[A](a: A): Partial[A] = a.left[Nu[A \/ ?]].embed
    def later[A](partial: Partial[A]): Partial[A] = partial.right[A].embed

    def delay[A](a: A): Option ~> (A \/ ?) = new (Option ~> (A \/ ?)) {
      def apply[B](b: Option[B]) = b \/> a
    }

    /** Canonical function that diverges.
      */
    def never[A]: Partial[A] = ().ana[Nu, A \/ ?](_.right)

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
    import Partial._

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

    /** If two `Partial`s eventually have the same value, then they are
      * equivalent.
      */
    def ≈(that: Partial[A])(implicit A: Equal[A]): Partial[Boolean] =
      // NB: could be defined as `(self ⊛ that)(_ ≟ _)`
      (self, that).ana[Nu, Boolean \/ ?](_.bimap(_.project, _.project) match {
        case (-\/(a1), -\/(a2)) => (a1 ≟ a2).left
        case (\/-(l1), -\/(a2)) => (l1,      now(a2)).right
        case (-\/(a1), \/-(l2)) => (now(a1), l2).right
        case (\/-(l1), \/-(l2)) => (l1,      l2).right
      })
  }
}
