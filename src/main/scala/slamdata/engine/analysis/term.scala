package slamdata.engine

import scalaz.{Functor, Cofree, Foldable, Show, Cord, Tree => ZTree, Monad, Traverse}

import scalaz.Tags.Disjunction

import scalaz.std.anyVal._
import scalaz.std.list._

import scalaz.syntax.monad._

final case class Term[F[_]](unFix: F[Term[F]]) {
  def cofree(implicit f: Functor[F]): Cofree[F, Unit] = {
    Cofree(Unit, Functor[F].map(unFix)(_.cofree))
  }

  def isLeaf(implicit F: Foldable[F]): Boolean = {
    F.foldMap(unFix)(Function.const(Disjunction(true)))
  }

  def children(implicit F: Foldable[F]): List[Term[F]] = {
    F.foldMap(unFix)(_ :: Nil)
  }

  def universe(implicit F: Foldable[F]): List[Term[F]] = {
    for {
      child <- children
      desc  <- child.universe
    } yield desc
  }

  def transform(f: Term[F] => Term[F])(implicit F: Functor[F]): Term[F] = {
    Term(F.map(unFix)(_ transform f))
  }

  def transformM[M[_]](f: Term[F] => M[Term[F]])(implicit M: Monad[M], TraverseF: Traverse[F]): M[Term[F]] = {
    def loop(term: Term[F]): M[Term[F]] = {
      for {
        y <- TraverseF.traverse(unFix)(loop _)
        z <- f(Term(y))
      } yield z
    }

    loop(this)    
  }

  def topDownTransform(f: Term[F] => Term[F])(implicit F: Functor[F]): Term[F] = {
    Term[F](F.map(unFix)(f).map(_ topDownTransform f))
  }

  def topDownTransformM[M[_]](f: Term[F] => M[Term[F]])(implicit M: Monad[M], TraverseF: Traverse[F]): M[Term[F]] = {
    def loop(term: Term[F]): M[Term[F]] = {
      for {
        x <- f(term)
        y <- TraverseF.traverse(x.unFix)(loop _)
      } yield Term(y)
    }

    loop(this)
  }

  def descend(f: Term[F] => Term[F])(implicit F: Functor[F]): Term[F] = {
    Term(F.map(unFix)(f))
  }

  def descendM[M[_]](f: Term[F] => M[Term[F]])(implicit M: Monad[M], TraverseF: Traverse[F]): M[Term[F]] = {
    TraverseF.traverse(unFix)(f).map(Term.apply _)
  }
}

trait TermDemo {
  sealed trait Node[A]
  case class Add[A](left: A, right: A) extends Node[A]
  case class Int[A]() extends Node[A]

  def node(v: Node[Term[Node]]): Term[Node] = Term[Node](v)

  node(Add(node(Int()), node(Int()))) 
}

object Term {
  case class Ann[F[_], A, B](attr: A, unAnn: F[B])

  sealed trait CoAnn[F[_], A, B]
  object CoAnn {
    case class Pure[F[_], A, B](attr: A) extends CoAnn[F, A, B]
    case class UnAnn[F[_], A, B](unAnn: F[B]) extends CoAnn[F, A, B]
  }

  type CoAttr[F[_], A] = Term[({type f[b] = CoAnn[F, A, b]})#f]

  def liftAnn[F[_], G[_], A, E](f: F[E] => G[E], ann: Ann[F, A, E]): Ann[G, A, E] = Ann(ann.attr, f(ann.unAnn))

  def liftCoAnn[F[_], G[_], A, E](f: F[E] => G[E], coann: CoAnn[F, A, E]): CoAnn[G, A, E] = coann match {
    case CoAnn.Pure(attr) => CoAnn.Pure(attr)
    case CoAnn.UnAnn(unAnn) => CoAnn.UnAnn(f(unAnn))
  }

  type Attr[F[_], A] = Term[({type f[b]=Ann[F, A, b]})#f]

  def attr[F[_], A](attr: Attr[F, A]): A = attr.unFix.attr

  def forget[F[_], A](attr: Attr[F, A])(implicit F: Functor[F]): Term[F] = Term(F.map(attr.unFix.unAnn)(forget[F, A](_)))

  case object Hole

  implicit def TermShow[F[_]](implicit showF: Show[F[Term[F]]], foldF: Foldable[F]) = new Show[Term[F]] {
    override def show(term: Term[F]): Cord = {
      def toTree(term: Term[F]): ZTree[F[Term[F]]] = {
        ZTree.node(term.unFix, term.children.toStream.map(toTree _))
      }

      Cord(toTree(term).drawTree)
    }
  }
}

