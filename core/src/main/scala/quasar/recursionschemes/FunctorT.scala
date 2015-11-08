/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.recursionschemes

import quasar.Predef._
import quasar.fp._

import scalaz._, Scalaz._
import simulacrum.{typeclass, op}

/** This provides more restricted forms of folds and unfolds than Recursive and
  * Corecursive, but in exchange, they are available to  more types. It helps
  * reduce constraints when creating [co]algebras parameterized on `T`.
  */
@typeclass trait FunctorT[T[_[_]]] {
  @op("∘") def map[F[_], G[_]](t: T[F])(f: F[T[F]] => G[T[G]]): T[G]

  /** The distinction between this and `transCata` is that this allows the
    * algebra to return the context (the outer `T`) to be used, whereas
    * `transCata` always uses the original context of the argument to `f`.
    *
    * This is noticable when `T` is `Cofree`. In this function, the result may
    * have any `head` the algebra desires, whereas in `transCata`, it can only
    * have the `head` of the argument to `f`.
    */
  def transCataT[F[_]: Functor](t: T[F])(f: T[F] => T[F]): T[F] =
    f(map(t)(_.map(transCataT(_)(f))))

  /** transAnaT : transAna :: transCataT : transCata */
  def transAnaT[F[_]: Functor](t: T[F])(f: T[F] => T[F]): T[F] =
    map(f(t))(_.map(transAnaT(_)(f)))

  def transCata[F[_]: Functor, G[_]](t: T[F])(f: F[T[G]] => G[T[G]]): T[G] =
    map(t)(ft => f(ft.map(transCata(_)(f))))

  def transAna[F[_], G[_]: Functor](t: T[F])(f: F[T[F]] => G[T[F]]): T[G] =
    map(t)(f(_).map(transAna(_)(f)))

  def transPara[F[_]: Functor, G[_]](t: T[F])(f: F[(T[F], T[G])] => G[T[G]]):
      T[G] =
    map(t)(ft => f(ft.map(tf => (tf, transPara(tf)(f)))))

  def transApo[F[_], G[_]: Functor](t: T[F])(f: F[T[F]] => G[T[G] \/ T[F]]):
      T[G] =
    map(t)(f(_).map(_.fold(ι, transApo(_)(f))))

  def translate[F[_], G[_]: Functor](t: T[F])(f: F ~> G): T[G] =
    map(t)(f(_).map(translate(_)(f)))

  def topDownCata[F[_]: Functor, A](t: T[F], a: A)(f: (A, T[F]) => (A, T[F])):
      T[F] = {
    val (a0, tf) = f(a, t)
    map(tf)(_.map(topDownCata(_, a0)(f)))
  }
}
