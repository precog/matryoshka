package quasar.recursionschemes

import scalaz.{Tree => ZTree, _}
import scalaz.std.list._
import scalaz.syntax.zip._

sealed trait Diff[F[_]]
object Diff {
  /** This branch is the same all the way to the tips of its leaves */
  case class Same[F[_]](ident: F[Fix[F]]) extends Diff[F]
  /** This node is the same, but it has at least one branch that differs */
  case class Similar[F[_]](ident: F[Fix[F]]) extends Diff[F]
  /** This node differs, but only in its parameters, not its shape. */
  // TODO: We have a matrix of
  //       unchanged           | non-recursively-changed
  //       recursively-changed | ??? – should we fill in this quad?
  case class LocallyDifferent[F[_]](left: F[Diff[F]], right: F[Unit])
      extends Diff[F]
  /** This node exists only in the right tree, but some descendent is similar */
  // TODO: Should we combine Inserted/Deleted and Added/Removed into something
  //       like Spliced(+/-) … and I dunno what for Added/Removed
  case class Inserted[F[_]](right: F[Diff[F]]) extends Diff[F]
  /** This node exists only in the left tree, but some descendent is similar. */
  case class Deleted[F[_]](left: F[Diff[F]]) extends Diff[F]
  /** This branch exists only in the right tree. */
  case class Added[F[_]](right: F[Fix[F]]) extends Diff[F]
  /** This branch exists only in the left tree. */
  case class Removed[F[_]](left: F[Fix[F]]) extends Diff[F]
  /** This node differs between the two trees */
  case class Different[F[_]](left: F[Fix[F]], right: F[Fix[F]])
      extends Diff[F]
  /** This node has some incomparable value. */
  case class Unknown[F[_]](left: F[Diff[F]], right: F[Unit])
      extends Diff[F]

  implicit def ShowDiff[F[_]: Foldable](implicit showF: Show[F[_]]) =
    new Show[Diff[F]] {
      override def show(v: Diff[F]): Cord = {
        def toTree(term: Diff[F]): ZTree[Cord] = term match {
          case Same(_) => ZTree.node(Cord("..."), Nil.toStream)
          case Similar(x) =>
            ZTree.node(
              showF.show(x),
              foldF.foldMap(x)(_ :: Nil).toStream.map(toTree _))
          case Different(l, r) =>
            ZTree.node(
              Cord("vvvvvvvvv left  vvvvvvvvv\n") ++
                Show[Term[F]].show(Term(l)) ++
                Cord("=========================\n") ++
                Show[Term[F]].show(Term(r)) ++
                Cord("^^^^^^^^^ right ^^^^^^^^^"),
              Nil.toStream)
          case Inserted(x) =>
            ZTree.node(
              Cord("+++> ") ++ showF.show(x),
              foldF.foldMap(x)(_ :: Nil).toStream.map(toTree _))
          case Deleted(x) =>
            ZTree.node(
              Cord("<--- ") ++ showF.show(x),
              foldF.foldMap(x)(_ :: Nil).toStream.map(toTree _))
          case Added(x) =>
            ZTree.node(
              Cord("+++> ") ++ Show[Term[F]].show(Term(x)),
              Nil.toStream)
          case Removed(x) =>
            ZTree.node(
              Cord("<--- ") ++ Show[Term[F]].show(Term(x)),
              Nil.toStream)
          case LocallyDifferent(l, r) =>
            ZTree.node(
              showF.show(l) ++ " <=/=> " ++ showF.show(r),
              foldF.foldMap(l)(_ :: Nil).toStream.map(toTree _))
          case Unknown(l, r) =>
            ZTree.node(
              showF.show(l) ++ " <???> " ++ showF.show(r),
              foldF.foldMap(l)(_ :: Nil).toStream.map(toTree _))
        }

        Cord(toTree(v).drawTree)
      }
    }
}

@typeclass trait Diffable[F[_]: Functor: Foldable: Merge] {
  import Diff._

  type DiffImpl[T[_[_]], F[_]] = PartialFunction[(F[T[F]], F[T[F]]), Diff[F]]
  val diffImpl[T[_[_]]: Recursive]: DiffImpl[T, F]

  def diff[T[_[_]]: Recursive](left: T[F], right: T[F]): Diff[F] =
    left.paraMerga(right)((l, r, merged: Option[F[Diff[F]]]) =>
      merged match {
        case None         => (diffImpl.lift)(l, r).getOrElse(Different(l, r))
        case Some(merged) =>
          val children = merged.foldMap(List(_))
          if (children.length == children.collect { case Same(_) => () }.length)
            Same(l)
          else Similar(merged)
      })

  /**
    Useful when a case class has a `List[A]` that isn’t the final `A`. This is
    because the normal comparison just walks over the children of the functor,
    so if the lists are different lengths, the default behavior will be
    confusing.

    Currently also useful when the only list _is_ the final parameter, because
    it allows you to explicitly use `Similar` rather than `LocallyDifferent`.
    */
  def diffLists[T[_[_]]: Recursive](left: List[T[F]], right: List[T[F]]):
      List[Diff[F]] =
    if (left.length < right.length)
      left.zipWithR(right) {
        case (Some(l), r) => diff(l, r)
        case (None,    r) => Added(project(r))
      }
      else
        left.zipWithL(right) {
          case (l, Some(r)) => diff(l, r)
          case (l, None)    => Removed(project(l))
        }

  // TODO: create something like Equals, but that overrides G[F[_]] (where G
  //       implements Traverse) to always be equal. This should allow us to
  //       distinguish between, say, two things containing a List[F[_]] that
  //       only differ on the length of the list. So we can make them `Similar`
  //       rather than `LocallyDifferent`.

  def localDiff[T[_[_]]: Recursive](left: F[T[F]], right: F[T[F]])(implicit FT: Traverse[F]):
      Diff[F] =
    LocallyDifferent(
      if (left.foldMap(List(_)).length < right.foldMap(List(_)).length)
        left.zipWithR(right) {
          case (Some(a), b) => diff(a, b)
          case (None,    b) => Added(project(b))
        }
        else
          left.zipWithL(right) {
            case (a, Some(b)) => diff(a, b)
            case (a, None)    => Removed(project(a))
          },
      right.void)
}
object Diffable {
  @inline def apply[F[_]](implicit F: Diffable[F]): Diffable[F] = F

  implicit def DiffableTraverse[F[_]: Traverse] = new Diffable[F] {
    val diffImpl[T[_[_]]: Recursive]: DiffImpl[T, F] = { case (l, r) =>
      // NB: This is it. The one hack in all of this: RTTI
      if (l.getClass == r.getClass) localDiff(l, r) else Diff.Different(l, r)
    }
  }
}
