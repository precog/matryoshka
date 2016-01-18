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

package quasar.fp
package free

import quasar.Predef._

import scalaz._
import scalaz.stream.Process

import scala.collection.IndexedSeq

/** Provides a range of natural transformations that can be derived from the natural transformation of a term
  * of an algebra into the desired `Monad`
  * @param interpretTerm A natural transformation from the Algebra into the desired `Monad` from which
  *                      many other natural transformations can be derived
  * @tparam F The type of the `Functor` that represents the algebra to be interpreted
  * @tparam M The `Monad` into which to translate the `Free` Algebra
  */
class Interpreter[F[_]: Functor, M[_]: Monad](val interpretTerm: F ~> M) {
  type Program[A] = Free[F,A]

  def interpret: Program ~> M =
    hoistFree(interpretTerm)

  def interpretT[T[_[_],_]: Hoist]: T[Program,?] ~> T[M,?] =
    Hoist[T].hoist[Program,M](interpret)

  def interpretT2[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist]: T1[T2[Program,?],?] ~> T1[T2[M,?],?] =
    Hoist[T1].hoist[T2[Program,?],T2[M,?]](interpretT[T2])(Hoist[T2].apply[Program])

  def interpretT3[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist, T3[_[_],_]: Hoist]: T1[T2[T3[Program,?],?],?] ~> T1[T2[T3[M,?],?],?] =
    Hoist[T1].hoist[T2[T3[Program,?],?], T2[T3[M,?],?]](interpretT2[T2,T3])(Hoist[T2].apply[T3[Program,?]](Hoist[T3].apply[Program]))

  def runLog[A](p: Process[Program,A])(implicit catchable: Catchable[M]): M[IndexedSeq[A]] =
    p.translate(interpret).runLog

  def runLogT[T[_[_],_]:Hoist,A](p: Process[T[Program,?],A])(implicit catchable: Catchable[T[M,?]]): T[M,IndexedSeq[A]] = {
    type ResultT[A] = T[M,A]
    val monadR: Monad[ResultT] = Hoist[T].apply
    p.translate[T[M,?]](interpretT[T]).runLog[ResultT,A](monadR, catchable)
  }

  def runLogT2[T1[_[_],_]: Hoist, T2[_[_],_]: Hoist,A](p: Process[T1[T2[Program,?],?],A])(implicit catchable: Catchable[T1[T2[M,?],?]]): T1[T2[M,?],IndexedSeq[A]] = {
    type ResultT[A] = T1[T2[M,?],A]
    val monadR: Monad[ResultT] = Hoist[T1].apply[T2[M,?]](Hoist[T2].apply)
    p.translate[ResultT](interpretT2[T1,T2]).runLog[ResultT,A](monadR,catchable)
  }

  def runLogT3[T1[_[_],_]:Hoist, T2[_[_],_]: Hoist, T3[_[_],_]: Hoist ,A](p: Process[T1[T2[T3[Program,?],?],?],A])(implicit catchable: Catchable[T1[T2[T3[M,?],?],?]])
  : T1[T2[T3[M,?],?],IndexedSeq[A]] = {
    type ResultT[A] = T1[T2[T3[M,?],?],A]
    val monadR: Monad[ResultT] = Hoist[T1].apply[T2[T3[M,?],?]](Hoist[T2].apply[T3[M,?]](Hoist[T3].apply))
    p.translate[ResultT](interpretT3[T1,T2,T3]).runLog[ResultT,A](monadR,catchable)
  }
}

/** Extends Interpreter to provide a version with better type inference for commonly used `MonadTrans`
  * @param interpretTerm A natural transformation from the Algebra into the desired `Monad` from which
  *                      many other natural transformations can be derived
  * @tparam F The type of the `Functor` that represents the algebra to be interpreted
  * @tparam M The `Monad` into which to translate the `Free` Algebra
  */
// https://github.com/puffnfresh/wartremover/issues/149
@SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
class SpecializedInterpreter[F[_]: Functor, M[_]: Monad](interpretTerm: F ~> M) extends Interpreter(interpretTerm) {
  def runLog[E,A](p: Process[EitherT[Program,E,?],A])(implicit catchable: Catchable[M]): EitherT[M,E,IndexedSeq[A]] = {
    type T[Program[_],A] = EitherT[Program,E,A]
    runLogT[T,A](p)
  }
  def runLog[E,L:Monoid,A](p: Process[EitherT[WriterT[Program,L,?],E,?],A])(implicit catchable: Catchable[M]): EitherT[WriterT[M,L,?],E,IndexedSeq[A]] = {
    type T1[M[_],A] = EitherT[M,E,A]
    type T2[M[_],A] = WriterT[M,L,A]
    type WriterResult[A] = T2[M,A]
    type ResultT[A] = T1[T2[M,?],A]
    val catchableR: Catchable[ResultT] = eitherTCatchable[WriterResult,E](writerTCatchable[M,L], WriterT.writerTFunctor[M,L])
    runLogT2[T1,T2,A](p)(Hoist[T1],Hoist[T2],catchableR)
  }
}
