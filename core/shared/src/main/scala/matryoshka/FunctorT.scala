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

import scala.Predef
import scalaz._, Scalaz._
import simulacrum._

/** The operations here are very similar to those in Recursive and Corecursive.
  * The usual names (`cata`, `ana`, etc.) are prefixed with `trans` and behave
  * generally the same, except
  * 1. the `A` of the [un]fold is restricted to `T[G]`, but
  * 2. the [co]algebra shape is like `F[A] => G[A]` rather than `F[A] => A`.
  *
  * In exchange, the [un]folds are available to more types (EG, folds available
  * to `Free` and unfolds available to `Cofree`).
  *
  * There are two additional operations – `transCataT` and `transAnaT`. The
  * distinction between these and `transCata`/`transAna` is that this allows
  * the algebra to return the context (the outer `T`) to be used, whereas
  * `transCata` always uses the original context of the argument to `f`.
  *
  * This is noticable when `T` is `Cofree`. In this function, the result may
  * have any `head` the algebra desires, whereas in `transCata`, it can only
  * have the `head` of the argument to `f`.
  *
  * Docs for operations, since they seem to break scaladoc if put in the right
  * place:
  *
  * - `map` – very roughly like Uniplate’s `descend`
  * - `transCataT` – akin to Uniplate’s `transform`
  * - `transAnaT` – akin to Uniplate’s `topDownTransform`
  * - `transCata` – akin to Fixplate’s `restructure`
  */
@typeclass trait FunctorT[T[_[_]]] {
  def mapT[F[_]: Functor, G[_]: Functor](t: T[F])(f: F[T[F]] => G[T[G]]):
      T[G]

  def transCataT[F[_]: Functor](t: T[F])(f: T[F] => T[F]): T[F] =
    f(mapT(t)(_.map(transCataT(_)(f))))

  /** This behaves like [[matryoshka.Recursive.elgotPara]]`, but it’s harder to
    * see from the types that in the tuple, `_2` is the result so far and `_1`
    * is the original structure.
    */
  def transParaT[F[_]: Functor](t: T[F])(f: ((T[F], T[F])) => T[F]): T[F] =
    f((t, mapT(t)(_.map(transParaT(_)(f)))))

  def transAnaT[F[_]: Functor](t: T[F])(f: T[F] => T[F]): T[F] =
    mapT(f(t))(_.map(transAnaT(_)(f)))

  /** This behaves like [[matryoshka.Corecursive.elgotApo]]`, but it’s harder to
    * see from the types that in the disjunction, `-\/` is the final result for
    * this node, while `\/-` means to keep processing the children.
    */
  def transApoT[F[_]: Functor](t: T[F])(f: T[F] => T[F] \/ T[F]): T[F] =
    f(t).fold(Predef.identity, mapT(_)(_.map(transApoT(_)(f))))

  def transHyloT[F[_]: Functor](t: T[F])(φ: T[F] => T[F], ψ: T[F] => T[F]):
      T[F] =
    φ(mapT(ψ(t))(_ ∘ (transHyloT(_)(φ, ψ))))

  def transCata[F[_]: Functor, G[_]: Functor](t: T[F])(f: AlgebraicTransform[T, F, G]): T[G] =
    mapT(t)(ft => f(ft.map(transCata(_)(f))))

  def transAna[F[_]: Functor, G[_]: Functor](t: T[F])(f: CoalgebraicTransform[T, F, G]): T[G] =
    mapT(t)(f(_).map(transAna(_)(f)))

  def transPrepro[F[_]: Functor, G[_]: Functor](t: T[F])(e: F ~> F, f: AlgebraicTransform[T, F, G]): T[G] =
    mapT(t)(ft => f(ft ∘ (x => transPrepro(transCata[F, F](x)(e(_)))(e, f))))

  def transPostpro[F[_]: Functor, G[_]: Functor](t: T[F])(e: G ~> G, f: CoalgebraicTransform[T, F, G]): T[G] =
    mapT(t)(f(_) ∘ (x => transAna(transPostpro(x)(e, f))(e)))

  def transPara[F[_]: Functor, G[_]: Functor](t: T[F])(f: GAlgebraicTransform[T, (T[F], ?), F, G]):
      T[G] =
    mapT(t)(ft => f(ft.map(tf => (tf, transPara(tf)(f)))))

  def transApo[F[_]: Functor, G[_]: Functor](t: T[F])(f: GCoalgebraicTransform[T, (T[G] \/ ?), F, G]):
      T[G] =
    mapT(t)(f(_).map(_.fold(Predef.identity, transApo(_)(f))))

  def transHylo[F[_]: Functor, G[_]: Functor, H[_]: Functor](
    t: T[F])(
    φ: G[T[H]] => H[T[H]], ψ: F[T[F]] => G[T[F]]):
      T[H] =
    mapT(t)(ft => φ(ψ(ft) ∘ (transHylo(_)(φ, ψ))))

  def topDownCata[F[_]: Functor, A](t: T[F], a: A)(f: (A, T[F]) => (A, T[F])):
      T[F] = {
    val (a0, tf) = f(a, t)
    mapT(tf)(_.map(topDownCata(_, a0)(f)))
  }
}

object FunctorT {
  implicit def birecursiveT[T[_[_]]: RecursiveT: CorecursiveT]: FunctorT[T] =
    new FunctorT[T] {
      def mapT[F[_], G[_]](t: T[F])(f: F[T[F]] => G[T[G]])(implicit F: Functor[F], G: Functor[G]) =
        CorecursiveT[T].embedT(f(RecursiveT[T].projectT(t)))
    }
}
