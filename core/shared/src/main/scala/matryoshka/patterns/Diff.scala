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

import scala.Unit

import scalaz._, Scalaz._

/** Represents diffs of recursive data structures.
  */
sealed trait Diff[T[_[_]], F[_], A]
final case class Same[T[_[_]], F[_], A](ident: T[F]) extends Diff[T, F, A]
final case class Similar[T[_[_]], F[_], A](ident: F[A]) extends Diff[T, F, A]
final case class Different[T[_[_]], F[_], A](left: T[F], right: T[F])
    extends Diff[T, F, A]
final case class LocallyDifferent[T[_[_]], F[_], A](left: F[A], right: F[Unit])
    extends Diff[T, F, A]
final case class Inserted[T[_[_]], F[_], A](right: F[A])
    extends Diff[T, F, A]
final case class Deleted[T[_[_]], F[_], A](left: F[A])
    extends Diff[T, F, A]
final case class Added[T[_[_]], F[_], A](right: T[F])
    extends Diff[T, F, A]
final case class Removed[T[_[_]], F[_], A](left: T[F])
    extends Diff[T, F, A]

object Diff extends DiffInstances

sealed abstract class DiffInstances extends DiffInstances0 {
  // TODO: implement low-prio Foldable with looser constraint on F
  implicit def traverse[T[_[_]], F[_]: Traverse]: Traverse[Diff[T, F, ?]] =
    new Traverse[Diff[T, F, ?]] {
      def traverseImpl[G[_], A, B](
        fa: Diff[T, F, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[Diff[T, F, B]] =
        fa match {
          case Same(ident) => G.point(Same(ident))
          case Similar(ident) => ident.traverse(f) ∘ (Similar(_))
          case Different(left, right) =>
            G.point(Different(left, right))
          case LocallyDifferent(left, right) =>
            left.traverse(f) ∘ (LocallyDifferent(_, right))
          case Inserted(right) => right.traverse(f) ∘ (Inserted(_))
          case Deleted(left) => left.traverse(f) ∘ (Deleted(_))
          case Added(right) => G.point(Added(right))
          case Removed(left) => G.point(Removed(left))
        }
    }

  implicit def equal[T[_[_]], F[_]](
    implicit T: Equal[T[F]], F: Delay[Equal, F]):
      Delay[Equal, Diff[T, F, ?]] =
    new Delay[Equal, Diff[T, F, ?]] {
      def apply[α](eq: Equal[α]) =
        Equal.equal((a, b) => (a, b) match {
          case (Same(i1), Same(i2)) => T.equal(i1, i2)
          case (Similar(i1), Similar(i2)) => F(eq).equal(i1, i2)
          case (Different(l1, r1), Different(l2, r2)) =>
            T.equal(l1, l2) && T.equal(r1, r2)
          case (LocallyDifferent(l1, r1), LocallyDifferent(l2, r2)) =>
            F(eq).equal(l1, l2) && F(Equal[Unit]).equal(r1, r2)
          case (Inserted(r1), Inserted(r2)) => F(eq).equal(r1, r2)
          case (Deleted(l1), Deleted(l2)) => F(eq).equal(l1, l2)
          case (Added(r1), Added(r2)) => T.equal(r1, r2)
          case (Removed(l1), Removed(l2)) => T.equal(l1, l2)
          case (_, _) => false
        })
    }

  implicit def show[T[_[_]], F[_]: Functor: Foldable](
    implicit T: Show[T[F]], F: Delay[Show, F]):
      Delay[Show, Diff[T, F, ?]] =
    new Delay[Show, Diff[T, F, ?]] {
      def apply[α](s: Show[α]) = Show.show {
        case Same(_) => Cord("...")
        case Similar(x) => F(s).show(x)
        case Different(l, r) =>
          Cord("vvvvvvvvv left  vvvvvvvvv\n") ++
            T.show(l) ++
            Cord("\n=========================\n") ++
            T.show(r) ++
            Cord("\n^^^^^^^^^ right ^^^^^^^^^")
        case Inserted(x) =>  Cord("+++> ") ++ F(s).show(x)
        case Deleted(x) => Cord("<--- ") ++ F(s).show(x)
        case Added(x) => Cord("+++> ") ++ T.show(x)
        case Removed(x) => Cord("<--- ") ++ T.show(x)
        case LocallyDifferent(l, r) =>
          F(s).show(l) ++ " <=/=> " ++ F(Show[Unit]).show(r)
      }
    }
}

sealed abstract class DiffInstances0 {
  implicit def functor[T[_[_]], F[_]: Functor]: Functor[Diff[T, F, ?]] =
    new Functor[Diff[T, F, ?]] {
      def map[A, B](fa: Diff[T, F, A])(f: A => B): Diff[T, F, B] =
        fa match {
          case Same(ident)                   => Same(ident)
          case Similar(ident)                => Similar(ident ∘ f)
          case Different(left, right)        => Different(left, right)
          case LocallyDifferent(left, right) =>
            LocallyDifferent(left ∘ f, right)
          case Inserted(right)               => Inserted(right ∘ f)
          case Deleted(left)                 => Deleted(left ∘ f)
          case Added(right)                  => Added(right)
          case Removed(left)                 => Removed(left)
        }
    }
}
