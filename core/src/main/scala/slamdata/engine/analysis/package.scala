package slamdata.engine

import slamdata.engine.analysis.fixplate._

import scalaz._

package object analysis {
  object -> {
    def unapply[A, B](value: (A, B)) = Some(value)
  }

  type -> [A, B] = (A, B)

  // FIXME: This is just cata … but then it can fail?
  type Analyzer[F[_], A, E] = (F[A] => A, Term[F]) => Validation[E, A]

  type AnalysisResult[F[_], A, E] = Validation[E, Cofree[F, A]]

  type Analysis[F[_], A, B, E] = Cofree[F, A] => AnalysisResult[F, B, E]

  implicit def AnalysisArrow[N[_]: Traverse, E] = new Arrow[({type f[a, b] = Analysis[N, a, b, E]})#f] {
    def arr[A, B](f: (A) => B): Analysis[N, A, B, E] =
      tree => Validation.success(tree.map(f))

    def compose[A, B, C](f: Analysis[N, B, C, E], g: Analysis[N, A, B, E]): Analysis[N, A, C, E] =
      g(_).fold(Validation.failure, f)

    def first[A, B, C](f: Analysis[N, A, B, E]): Analysis[N, (A, C), (B, C), E] =
      treeAC => f(treeAC.map(_._1)).map(unsafeZip2(_, treeAC.map(_._2)))

    def id[A]: Analysis[N, A, A, E] = Validation.success(_)
  }

  implicit def AnalysisFunctor[N[_]: Functor, A, E] =
    new Functor[({type f[b] = Analysis[N, A, b, E]})#f] {
      def map[B, C](fa: Analysis[N, A, B, E])(f: (B) => C) = fa(_).map(_.map(f))
  }

  implicit class AnalysisW[N[_]: Traverse, A, B, E](self: Analysis[N, A, B, E]) {
    def >>> [C](that: Analysis[N, B, C, E]) = AnalysisArrow[N, E].compose(that, self)

    def <<< [C](that: Analysis[N, C, A, E]) = AnalysisArrow[N, E].compose(self, that)

    final def first[C]: Analysis[N, (A, C), (B, C), E] = AnalysisArrow[N, E].first(self)

    final def second[C]: Analysis[N, (C, A), (C, B), E] = AnalysisArrow[N, E].second(self)

    final def *** [C, D](k: Analysis[N, C, D, E]): Analysis[N, (A, C), (B, D), E] = AnalysisArrow[N, E].splitA(self, k)

    final def &&& [C](k: Analysis[N, A, C, E]): Analysis[N, A, (B, C), E] = AnalysisArrow[N, E].combine(self, k)

    final def product: Analysis[N, (A, A), (B, B), E] = AnalysisArrow[N, E].product(self)

    final def push[C](c: C): Analysis[N, A, (B, C), E] = AnalysisFunctor[N, A, E].map(self)(b => (b, c))
  }

  implicit class AnalysisW1To2[N[_]: Functor, A, B, C, E](self: Analysis[N, A, (C, B), E]) {
    final def dup2: Analysis[N, A, (((C, B), B), B), E] =
      AnalysisFunctor.map(self) { case (t, h) => (((t, h), h), h) }
  }

  implicit class AnalysisW2To1[N[_]: Traverse, A, B, C, E](self: Analysis[N, (B, A), C, E]) {
    final def pop2[D]: Analysis[N, ((D, B), A), (D, C), E] = {
      val c = AnalysisArrow[N, E].second[(B, A), C, D](self)
      AnalysisArrow[N, E].mapfst[(D, (B, A)), (D, C), ((D, B), A)](c) { case ((d, b), a) => (d, (b, a)) }
    }
  }
}
