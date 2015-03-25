package slamdata.engine.analysis

import Function.untupled

import scalaz._

import scalaz.std.list._
import scalaz.std.vector._
import scalaz.std.map._
import scalaz.std.tuple._
import scalaz.std.iterable._

import scala.collection.JavaConverters._

object Analysis {
  def readTree[N[_], A, B, E](f: Cofree[N, A] => Analysis[N, A, B, E]):
      Analysis[N, A, B, E] =
    tree => f(tree)(tree)

  // def annotate[N[_], A, B, E: Semigroup](f: N[A] => Validation[E, B]):
  //     Analysis[N, A, B, E] = tree => {
  //   Traverse[List].sequence[({type f[a]=Validation[E, a]})#f, (N, B)](tree.nodes.map(n => f(n).map(b => (n, b)))).map { list =>
  //     tree.annotate(list.toMap)
  //   }
  // }

  // def fork[N[_], A, B, E: Semigroup](analyzer: Analyzer[N, B, E]): Analysis[N, A, B, E] = tree => {
  //   // Analyzer is pure so for a given node, it doesn't matter which annotation we select:
  //   implicit val sg = Semigroup.firstSemigroup[B]

  //   tree.fork(new java.util.IdentityHashMap[N, B])({ (acc, node) =>
  //     analyzer((k: N) => acc.get(k), node).map(b => { acc.put(node, b); acc })
  //   }).map { vector =>
  //     val collapsed = vector.foldLeft(new java.util.IdentityHashMap[N, B]) { (acc, map) => acc.putAll(map); acc }
  //     tree.map(collapsed.get)
  //   }
  // }

  // def join[N[_], A, B, E: Semigroup](analyzer: Analyzer[N, B, E]): Analysis[N, A, B, E] = tree => {
  //   // Analyzer is pure so for a given node, it doesn't matter which annotation we select:
  //   implicit val sg = Semigroup.firstSemigroup[B]

  //   (tree.join(Map.empty[N, B])((acc: Map[N, B], node: N) => {
  //     analyzer(acc.apply, node).map { b =>
  //       acc + (node -> b)
  //     }
  //   })).map(tree.annotate _)
  // }

  // def loop[N[_], A, E: Semigroup](condition: Cofree[N, A] => Boolean)
  //   (push: Analyzer[N, A, E], pull: Analyzer[N, A, E]): Analysis[N, A, A, E] = {
  //   def loopWhile0(tree: Cofree[N, A]): Validation[E, Cofree[N, A]] = {
  //     if (!condition(tree)) Validation.success(tree)
  //     else {
  //       fork[N, A, A, E](push)(Semigroup[E])(tree).fold(
  //         Validation.failure,
  //         tree2 => {
  //           join[N, A, A, E](pull)(Semigroup[E])(tree2)
  //         }
  //       )
  //     }
  //   }

  //   loopWhile0
  // }

  def drop1[N[_]: Functor, A, B, E]: Analysis[N, (A, B), B, E] =
    tree => Validation.success(tree.map(_._2))

  def take1[N[_]: Functor, A, B, E]: Analysis[N, (A, B), A, E] =
    tree => Validation.success(tree.map(_._1))

  def flip[N[_]: Functor, A, B, E]: Analysis[N, (A, B), (B, A), E] =
    tree => Validation.success(tree.map(n => (n._2, n._1)))
}
