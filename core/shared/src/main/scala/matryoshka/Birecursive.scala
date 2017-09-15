/*
 * Copyright 2014–2017 SlamData Inc.
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

import matryoshka.patterns.EnvT

import scala.{Option, Unit}

import scalaz._, Scalaz._

/** A type that is both [[Recursive]] and [[Corecursive]].
  */
trait Birecursive[T] extends Recursive[T] with Corecursive[T] {
  /** Roughly a default impl of `project`, given a [[matryoshka.Corecursive]]
    * instance and an overridden `cata`.
    */
  def lambek(tf: T)(implicit BF: Functor[Base]): Base[T] =
    cata[Base[T]](tf)(_ ∘ embed)

  /** Roughly a default impl of `embed`, given a [[matryoshka.Recursive]]
    * instance and an overridden `ana`.
    */
  def colambek(ft: Base[T])(implicit BF: Functor[Base]): T =
    ana(ft)(_ ∘ project)

  def postpro[A](
    a: A)(
    e: Base ~> Base, g: Coalgebra[Base, A])(
    implicit BF: Functor[Base]):
      T =
    gpostpro[Id, A](a)(distAna, e, g)

  def gpostpro[N[_], A](
    a: A)(
    k: DistributiveLaw[N, Base], e: Base ~> Base, ψ: GCoalgebra[N, Base, A])(
    implicit BF: Functor[Base], N: Monad[N]):
      T =
    hylo[Yoneda[Base, ?], N[A], T](
      a.point[N])(
      fa => embed((fa ∘ (ana(_)(x => e(project(x))))).run),
        ma => Yoneda(k(ma ∘ ψ)) ∘ (_.join))

  override def para[A]
    (t: T)
    (f: GAlgebra[(T, ?), Base, A])
    (implicit BF: Functor[Base]) =
    gcata[(T, ?), A](t)(distPara, f)

  override def elgotPara[A]
    (t: T)
    (f: ElgotAlgebra[(T, ?), Base, A])
    (implicit BF: Functor[Base]) =
    elgotCata[(T, ?), A](t)(distPara, f)

  override def paraZygo[A, B]
    (t: T)
    (f: GAlgebra[(T, ?), Base, B], g: GAlgebra[(B, ?), Base, A])
    (implicit BF: Functor[Base], BU: Unzip[Base]) =
    gcataZygo[(T, ?), A, B](t)(distPara, f, g)

  override def apo[A]
    (a: A)
    (f: GCoalgebra[T \/ ?, Base, A])
    (implicit BF: Functor[Base]) =
    gana[T \/ ?, A](a)(distApo, f)

  override def elgotApo[A]
    (a: A)
    (f: ElgotCoalgebra[T \/ ?, Base, A])
    (implicit BF: Functor[Base]) =
    elgotAna[T \/ ?, A](a)(distApo, f)

  def gpara[W[_]: Comonad, A](
    t: T)(
    e: DistributiveLaw[Base, W], f: GAlgebra[EnvT[T, W, ?], Base, A])(
    implicit BF: Functor[Base]):
      A =
    gzygo[W, A, T](t)(embed, e, f)

  def prepro[A]
    (t: T)
    (e: Base ~> Base, f: Algebra[Base, A])
    (implicit BF: Functor[Base])
      : A =
    gprepro[Id, A](t)(distCata, e, f)

  def gprepro[W[_]: Comonad, A](
    t: T)(
    k: DistributiveLaw[Base, W], e: Base ~> Base, f: GAlgebra[W, Base, A])(
    implicit BF: Functor[Base]):
      A =
    hylo[Yoneda[Base, ?], T, W[A]](
      t)(
      fwa => k((fwa ∘ (_.cojoin)).run) ∘ f,
        t => Yoneda(project(t)) ∘ (cata[T](_)(c => embed(e(c))))).copoint

  // TODO: This should be an enrichment on Tuple2.
  private def sequenceTuple[F[_]: Functor, A, B](tup: (A, F[B])): F[(A, B)] =
    tup._2 ∘ ((tup._1, _))

  def topDownCata[A]
    (t: T, a: A)
    (f: (A, T) => (A, T))
    (implicit BF: Functor[Base])
      : T =
    ana((a, t))(at => sequenceTuple(f.tupled(at).map(project)))

  def topDownCataM[M[_]: Monad, A](
    t: T, a: A)(
    f: (A, T) => M[(A, T)])(
    implicit BT: Traverse[Base]):
      M[T] =
    anaM((a, t))(at => f.tupled(at).map(aft => sequenceTuple(aft.map(project))))

  override def transPara[U, G[_]: Functor]
    (t: T)
    (f: AlgebraicGTransform[(T, ?), U, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base]) =
    transGcata(t)(distPara, f)

  override def transApo[U, G[_]: Functor]
    (u: U)
    (f: CoalgebraicGTransform[T \/ ?, U, G, Base])
    (implicit U: Recursive.Aux[U, G], BF: Functor[Base])
      : T =
    transGana(u)(distApo, f)

  def transPrepro[U, G[_]: Functor]
    (t: T)
    (e: Base ~> Base, f: Transform[U, Base, G])
    (implicit U: Corecursive.Aux[U, G], BF: Functor[Base])
      : U =
    prepro(t)(e, f >>> (U.embed(_)))

  def transCataT
    (t: T)
    (f: T => T)
    (implicit BF: Functor[Base])
      : T =
    cata(t)(f <<< embed)

  /** This behaves like [[matryoshka.Recursive.elgotPara]]`, but it’s harder to
    * see from the types that in the tuple, `_2` is the result so far and `_1`
    * is the original structure.
    */
  def transParaT(t: T)(f: ((T, T)) => T)(implicit BF: Functor[Base]): T =
    elgotPara[T](t)(f <<< (_ ∘ embed))

  def transAnaT(t: T)(f: T => T)(implicit BF: Functor[Base]): T =
    ana(t)(f >>> project)

  /** This behaves like [[matryoshka.Corecursive.elgotApo]]`, but it’s harder to
    * see from the types that in the disjunction, `-\/` is the final result for
    * this node, while `\/-` means to keep processing the children.
    */
  def transApoT(t: T)(f: T => T \/ T)(implicit BF: Functor[Base]): T =
    elgotApo(t)(f(_) ∘ project)

  def transCataTM[M[_]: Monad](t: T)(f: T => M[T])(implicit BF: Traverse[Base])
      : M[T] =
    cataM(t)(f <<< embed)

  def transAnaTM[M[_]: Monad](t: T)(f: T => M[T])(implicit BF: Traverse[Base])
      : M[T] =
    anaM(t)(f(_) ∘ project)
}

object Birecursive {
  /** Create a [[Birecursive]] instance from the mappings to/from the
    * fixed-point.
    */
  def algebraIso[T, F[_]](φ: Algebra[F, T], ψ: Coalgebra[F, T])
      : Birecursive.Aux[T, F] =
    new Birecursive[T] {
      type Base[A] = F[A]
      def project(t: T)(implicit F: Functor[F]) = ψ(t)
      def embed(ft: F[T])(implicit F: Functor[F]) = φ(ft)
    }

  def iso[T, F[_]: Functor](implicit T: Birecursive.Aux[T, F]) =
    AlgebraIso[F, T](T.embed(_))(T.project(_))

  def lambekIso[T, F[_]: Functor](implicit T: Birecursive.Aux[T, F]) =
    AlgebraIso[F, T](T.colambek(_))(T.lambek(_))

  def equal[T, F[_]: Traverse]
    (implicit T: Birecursive.Aux[T, F], F: Equal[F[Unit]])
      : Equal[T] =
    Equal.equal((a, b) =>
      T.anaM[Option, (T, T)]((a, b)) {
        case (a, b) => Merge.fromTraverse[F].merge(T.project(a), T.project(b))
      }.isDefined)

  def order[T, F[_]: Traverse]
    (implicit T: Birecursive.Aux[T, F], F: Order[F[Unit]])
      : Order[T] =
    Order.order((a, b) =>
      T.anaM[Ordering \/ ?, (T, T)]((a, b)) {
        case (a, b) =>
          val (fa, fb) = (T.project(a), T.project(b))
          Merge.fromTraverse[F].merge(fa, fb) \/> (fa.void ?|? fb.void)
      }.as(Ordering.EQ).merge)

  // NB: The rest of this is what would be generated by simulacrum, except this
  //     type class is too complicated to take advantage of that.

  type Aux[T, F[_]] = Birecursive[T] { type Base[A] = F[A] }

  def apply[T](implicit instance: Birecursive[T]): Aux[T, instance.Base] =
    instance

  trait Ops[T, F[_]] {
    def typeClassInstance: Aux[T, F]
    def self: T

    def lambek(implicit BF: Functor[F]): F[T] =
      typeClassInstance.lambek(self)
    def gpara[W[_]: Comonad, A]
      (e: DistributiveLaw[F, W], f: GAlgebra[EnvT[T, W, ?], F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.gpara[W, A](self)(e, f)
    def prepro[A]
      (e: F ~> F, f: Algebra[F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.prepro[A](self)(e, f)
    def gprepro[W[_]: Comonad, A]
      (k: DistributiveLaw[F, W], e: F ~> F, f: GAlgebra[W, F, A])
      (implicit BF: Functor[F])
        : A =
      typeClassInstance.gprepro[W, A](self)(k, e, f)
    def topDownCata[A]
      (a: A)
      (f: (A, T) => (A, T))
      (implicit BF: Functor[F])
        : T =
      typeClassInstance.topDownCata[A](self, a)(f)
    def topDownCataM[M[_]: Monad, A]
      (a: A)
      (f: (A, T) => M[(A, T)])
      (implicit BT: Traverse[F])
        : M[T] =
      typeClassInstance.topDownCataM[M, A](self, a)(f)
    object transPrepro {
      def apply[U] = new PartiallyApplied[U]
      final class PartiallyApplied[U] {
        def apply[G[_]: Functor]
          (e: F ~> F, f: Transform[U, F, G])
          (implicit U: Corecursive.Aux[U, G], BF: Functor[F])
            : U =
          typeClassInstance.transPrepro(self)(e, f)
      }
    }
    def transCataT(f: T => T)(implicit BF: Functor[F]): T =
      typeClassInstance.transCataT(self)(f)

    def transParaT(f: ((T, T)) => T)(implicit BF: Functor[F]): T =
      typeClassInstance.transParaT(self)(f)

    def transAnaT(f: T => T)(implicit BF: Functor[F]): T =
      typeClassInstance.transAnaT(self)(f)

    def transApoT(f: T => T \/ T)(implicit BF: Functor[F]): T =
      typeClassInstance.transApoT(self)(f)

    def transCataTM[M[_]: Monad](f: T => M[T])(implicit BF: Traverse[F])
        : M[T] =
      typeClassInstance.transCataTM(self)(f)

    def transAnaTM[M[_]: Monad](f: T => M[T])(implicit BF: Traverse[F])
        : M[T] =
      typeClassInstance.transAnaTM(self)(f)
  }

  trait ToBirecursiveOps {
    implicit def toBirecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F]): Ops[T, F] =
      new Ops[T, F] {
        val self = target
        val typeClassInstance = tc
      }
  }

  object nonInheritedOps extends ToBirecursiveOps

  trait AllOps[T, F[_]] extends Ops[T, F] with Recursive.Ops[T, F] {
    def typeClassInstance: Aux[T, F]
  }

  object ops {
    implicit def toAllBirecursiveOps[T, F[_]](target: T)(implicit tc: Aux[T, F]): AllOps[T, F] =
      new AllOps[T, F] {
        val self = target
        val typeClassInstance = tc
      }
  }
}
