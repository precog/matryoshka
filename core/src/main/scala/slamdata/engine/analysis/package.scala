package slamdata.engine

import scalaz._

package object analysis {
  object -> {
    def unapply[A, B](value: (A, B)) = Some(value)
  }

  type -> [A, B] = (A, B)

  type Analyzer[N, A, E] = (N => A, N) => Validation[E, A]

  type AnalysisResult[N, A, E] = Validation[E, AnnotatedTree[N, A]]

  type Analysis[N, A, B, E] = AnnotatedTree[N, A] => AnalysisResult[N, B, E]

  implicit def AnalysisArrow[N, E] = new Arrow[({type f[a, b] = Analysis[N, a, b, E]})#f] {
    def arr[A, B](f: (A) => B): Analysis[N, A, B, E] = tree => Validation.success(tree.annotate(n => f(tree.attr(n))))

    def compose[A, B, C](f: Analysis[N, B, C, E], g: Analysis[N, A, B, E]): Analysis[N, A, C, E] =
      tree => g(tree).fold(
        Validation.failure,
        tree2 => f(tree2)
      )

    def first[A, B, C](f: Analysis[N, A, B, E]): Analysis[N, (A, C), (B, C), E] = treeAC =>
      f(treeAC.annotate(n => treeAC.attr(n)._1)).map(treeB => treeB.annotate(n => (treeB.attr(n), treeAC.attr(n)._2)))

    def id[A]: Analysis[N, A, A, E] = tree => Validation.success(tree)
  }

  implicit def AnalysisFunctor[N, A, E] = new Functor[({type f[b] = Analysis[N, A, b, E]})#f] {
    def map[B, C](fa: Analysis[N, A, B, E])(f: (B) => C): Analysis[N, A, C, E] = {
      (tree: AnnotatedTree[N, A]) => fa(tree).map(tree => tree.annotate(n => f(tree.attr(n))))
    }
  }

  implicit class AnalysisW[N, A, B, E](self: Analysis[N, A, B, E]) {
    def >>> [C](that: Analysis[N, B, C, E]) = AnalysisArrow[N, E].compose(that, self)

    def <<< [C](that: Analysis[N, C, A, E]) = AnalysisArrow[N, E].compose(self, that)

    final def first[C]: Analysis[N, (A, C), (B, C), E] = AnalysisArrow[N, E].first(self)

    final def second[C]: Analysis[N, (C, A), (C, B), E] = AnalysisArrow[N, E].second(self)

    final def *** [C, D](k: Analysis[N, C, D, E]): Analysis[N, (A, C), (B, D), E] = AnalysisArrow[N, E].splitA(self, k)

    final def &&& [C](k: Analysis[N, A, C, E]): Analysis[N, A, (B, C), E] = AnalysisArrow[N, E].combine(self, k)

    final def product: Analysis[N, (A, A), (B, B), E] = AnalysisArrow[N, E].product(self)

    final def push[C](c: C): Analysis[N, A, (B, C), E] = AnalysisFunctor[N, A, E].map(self)(b => (b, c))
  }

  implicit class AnalysisW1To2[N, A, B, C, E](self: Analysis[N, A, (C, B), E]) {
    final def dup2: Analysis[N, A, (((C, B), B), B), E] = AnalysisFunctor.map(self) { case (t, h) => (((t, h), h), h) }
  }

  implicit class AnalysisW2To1[N, A, B, C, E](self: Analysis[N, (B, A), C, E]) {
    final def pop2[D]: Analysis[N, ((D, B), A), (D, C), E] = {
      val c = AnalysisArrow[N, E].second[(B, A), C, D](self)
      AnalysisArrow.mapfst[(D, (B, A)), (D, C), ((D, B), A)](c) { case ((d, b), a) => (d, (b, a)) }
    }
  }
}
