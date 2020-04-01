/*
 * Copyright 2020 Precog Data
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

import org.scalacheck.Gen
import scalaz.Monad
import scalaz.Monoid

// cribbed from quasar
trait ScalazSpecs2Instances extends org.specs2.scalacheck.GenInstances {

  implicit def scalazGenMonad: Monad[Gen] = specs2ToScalazMonad(genMonad)

  // We cannot make this public implicit function since then it will conflict with e.g. `scalaz.idInstance`
  private def specs2ToScalazMonad[F[_]](specsMonad: org.specs2.fp.Monad[F]): Monad[F] = new Monad[F] {
    def point[A](a: => A): F[A] = specsMonad.point(a)
    def bind[A, B](fa: F[A])(f: A => F[B]): F[B] = specsMonad.bind(fa)(f)
  }

  implicit def specs2ToScalazMonoid[A](implicit specsMonoid: org.specs2.fp.Monoid[A]): Monoid[A] = {
    specs2ToScalazMonoidExplicit(specsMonoid)
  }

  implicit def specs2ToScalazMonoidExplicit[A](specsMonoid: org.specs2.fp.Monoid[A]): Monoid[A] = new Monoid[A] {
    override def zero: A = specsMonoid.zero
    override def append(f1: A, f2: => A): A = specsMonoid.append(f1, f2)
  }
}
