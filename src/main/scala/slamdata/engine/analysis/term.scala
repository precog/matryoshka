package slamdata.engine

import scalaz.{Apply, Applicative, Functor, Monoid, Cofree, Foldable, Show, Cord, Tree => ZTree, Monad, Traverse, Free, Arrow, Kleisli}

import scalaz.Tags.Disjunction

import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.function._

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

  def transform(f: Term[F] => Term[F])(implicit T: Traverse[F]): Term[F] = {
    transformM[Free.Trampoline]((v: Term[F]) => f(v).pure[Free.Trampoline]).run
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

  def topDownTransform(f: Term[F] => Term[F])(implicit T: Traverse[F]): Term[F] = {
    topDownTransformM[Free.Trampoline]((term: Term[F]) => f(term).pure[Free.Trampoline]).run
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

  def rewrite(f: Term[F] => Option[Term[F]])(implicit T: Traverse[F]): Term[F] = {
    rewriteM[Free.Trampoline]((term: Term[F]) => f(term).pure[Free.Trampoline]).run
  }

  def rewriteM[M[_]](f: Term[F] => M[Option[Term[F]]])(implicit M: Monad[M], TraverseF: Traverse[F]): M[Term[F]] = {
    transformM[M] { term =>
      for {
        x <- f(term)
        y <- Traverse[Option].traverse(x)(_ rewriteM f).map(_.getOrElse(term))
      } yield y
    }
  }

  def restructure[G[_]](f: F[Term[G]] => G[Term[G]])(implicit T: Traverse[F]): Term[G] = {
    restructureM[Free.Trampoline, G]((term: F[Term[G]]) => f(term).pure[Free.Trampoline]).run
  }

  def restructureM[M[_], G[_]](f: F[Term[G]] => M[G[Term[G]]])(implicit M: Monad[M], T: Traverse[F]): M[Term[G]] = {
    for {
      x <- T.traverse(unFix)(_ restructureM f)
      y <- f(x)
    } yield Term(y)
  }

  import attr._

  def context(implicit T: Traverse[F]): Attr[F, Term[F] => Term[F]] = {
    def loop(f: Term[F] => Term[F]): Attr[F, Term[F] => Term[F]] = {
      //def g(y: Term[F], replace: Term[F] => Term[F]) = loop()

      ???
    }

    loop(identity[Term[F]])
  }

}

trait holes {
  sealed trait Hole
  val Hole = new Hole{}

  def holes[F[_]: Traverse, A](fa: F[A]): F[(A, A => F[A])] = {
    (Traverse[F].mapAccumL(fa, 0) {
      case (i, x) =>
        val h: A => F[A] = { y =>
          val g: (Int, A) => (Int, A) = (j, z) => (j + 1, if (i == j) y else z)

          Traverse[F].mapAccumL(fa, 0)(g)._2
        }

        (i + 1, (x, h))
    })._2
  }

  def holesList[F[_]: Traverse, A](fa: F[A]): List[(A, A => F[A])] = Traverse[F].toList(holes(fa))

  def transformChildren[F[_]: Traverse, A](fa: F[A])(f: A => A): F[F[A]] = {
    val g: (A, A => F[A]) => F[A] = (x, replace) => replace(f(x))

    Traverse[F].map(holes(fa))(g.tupled)
  }

  def builder[F[_]: Traverse, A, B](fa: F[A], children: List[B]): F[B] = {
    (Traverse[F].mapAccumL(fa, children) {
      case (x :: xs, _) => (xs, x)
      case _ => sys.error("Not enough children")
    })._2
  }

  def project[F[_]: Foldable, A](index: Int, fa: F[A]): Option[A] = {
    ???
  }

  def sizeF[F[_]: Foldable, A](fa: F[A]): Int = Foldable[F].foldLeft(fa, 0)((a, b) => a + 1)
}

object holes extends holes

trait ShowF[F[_]] {
  def show[A: Show]: Show[F[A]]
}
object ShowF {
  final def apply[F[_]](implicit ShowF: ShowF[F]) = ShowF
}

trait TermInstances {
  implicit def TermShow[F[_]](implicit showF: Show[F[Term[F]]], foldF: Foldable[F]) = new Show[Term[F]] {
    override def show(term: Term[F]): Cord = {
      def toTree(term: Term[F]): ZTree[F[Term[F]]] = {
        ZTree.node(term.unFix, term.children.toStream.map(toTree _))
      }

      Cord(toTree(term).drawTree)
    }
  }
}
object Term extends TermInstances {
}

trait ann {
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
}

trait attr extends ann {
  type Attr[F[_], A] = Term[({type f[b]=Ann[F, A, b]})#f]

  def attr[F[_], A](attr: Attr[F, A]): A = attr.unFix.attr

  def forget[F[_], A](attr: Attr[F, A])(implicit F: Functor[F]): Term[F] = Term(F.map(attr.unFix.unAnn)(forget[F, A](_)))

  implicit def AttrFunctor[F[_]: Functor]: Functor[({type f[a]=Attr[F, a]})#f] = new Functor[({type f[a]=Attr[F, a]})#f] {
    def map[A, B](v: Attr[F, A])(f: A => B): Attr[F, B] = {
      type DestF[X] = Ann[F, B, X]

      Term[DestF](Ann(f(v.unFix.attr), Functor[F].map(v.unFix.unAnn)(t => AttrFunctor[F].map(t)(f))))
    }
  }

  implicit def AttrFoldable[F[_]: Foldable] = {
    type AttrFoldable[A] = Attr[F, A]

    new Foldable[AttrFoldable] {
      def foldMap[A, B](fa: AttrFoldable[A])(f: A => B)(implicit F: Monoid[B]): B = {
        val head = f(fa.unFix.attr)

        val tail = Foldable[F].foldMap(fa.unFix.unAnn)(v => foldMap(v)(f))

        Monoid[B].append(head, tail)
      }

      def foldRight[A, B](fa: AttrFoldable[A], z: => B)(f: (A, => B) => B): B = ???
    }
  }

  implicit def AttrTraverse[F[_]: Traverse] = {
    type AttrTraverse[A] = Attr[F, A]

    new Traverse[AttrTraverse] {
      def traverseImpl[G[_], A, B](fa: AttrTraverse[A])(f: A => G[B])(implicit G: Applicative[G]): G[AttrTraverse[B]] = {
        type AnnF[X] = Ann[F, A, X]
        type AnnF2[X] = Ann[F, B, X]

        val gb: G[B] = f(fa.unFix.attr)
        val gunAnn: G[F[AttrTraverse[B]]] = Traverse[F].traverseImpl(fa.unFix.unAnn)((v: Term[AnnF]) => traverseImpl(v)(f))

        G.apply2(gb, gunAnn)((b, unAnn) => Term[AnnF2](Ann(b, unAnn)))
      }
    }
  }

  def attrMap[F[_]: Functor, A, B](attr: Attr[F, A])(f: A => B): Attr[F, B] = {
    AttrFunctor[F].map(attr)(f)
  }

  def synthetize[F[_]: Functor, A](term: Term[F])(f: F[A] => A): Attr[F, A] = synthCata(term)(f)

  def synthCata[F[_]: Functor, A](term: Term[F])(f: F[A] => A): Attr[F, A] = {
    type AnnF[X] = Ann[F, A, X]

    val fattr: F[Attr[F, A]] = Functor[F].map(term.unFix)(t => synthCata(t)(f))

    val fa: F[A] = Functor[F].map(fattr)(attr _)

    Term[AnnF](Ann(f(fa), fattr))
  }

  def scanCata[F[_]: Functor, A, B](attr0: Attr[F, A])(f: (A, F[B]) => B): Attr[F, B] = {
    type AnnF[X] = Ann[F, B, X]

    val a : A = attr0.unFix.attr

    val unAnn = attr0.unFix.unAnn

    val fattr: F[Attr[F, B]] = Functor[F].map(unAnn)(t => scanCata(t)(f))

    val b : F[B] = Functor[F].map(fattr)(attr _)

    Term[AnnF](Ann(f(a, b), fattr))
  }

  def synthPara[F[_]: Functor, A](term: Term[F])(f: F[(Term[F], A)] => A): Attr[F, A] = {
    type AnnF[X] = Ann[F, A, X]

    def loop(term: Term[F]): (Term[F], Attr[F, A]) = {
      val rec : F[(Term[F], Attr[F, A])] = Functor[F].map(term.unFix)(loop _)

      val left = Functor[F].map(rec) {
        case (s, t) => (s, attr(t))
      }

      val right = Functor[F].map(rec)(_._2)

      (term, Term[AnnF](Ann(f(left), right)))
    }

    loop(term)._2
  }

  def synthPara2[F[_]: Functor, A](term: Term[F])(f: (Term[F], F[A]) => A): Attr[F, A] = {
    ???
  }

  def scanPara[F[_]: Functor, A, B](attr: Attr[F, A])(f: (Attr[F, A], F[B]) => B): Attr[F, B] = {
    ???
  }

  // def synthZygo_[F[_]: Functor, A, B]()

  /**
   * Zips two attributed nodes together. This is unsafe in the sense that the 
   * user is responsible to retain the shape of the node. 
   *
   * TODO: See if there's a safer less restrictive way of doing this.
   */
  def zip2[F[_]: Foldable: Traverse, A, B](left: Attr[F, A], right: Attr[F, B]): Attr[F, (A, B)] = {
    type AnnFA[X] = Ann[F, A, X]
    type AnnFB[X] = Ann[F, B, X]

    type AnnFAB[X] = Ann[F, (A, B), X]

    val lunFix = left.unFix

    val lattr: A = lunFix.attr
    val lunAnn: F[Term[AnnFA]] = lunFix.unAnn

    val lunAnnL: List[Term[AnnFA]] = Foldable[F].toList(lunAnn)

    val runFix = right.unFix 
    val rattr: B = runFix.attr
    val runAnn: F[Term[AnnFB]] = runFix.unAnn

    val runAnnL: List[Term[AnnFB]] = Foldable[F].toList(runAnn)

    val abs: List[Term[AnnFAB]] = lunAnnL.zip(runAnnL).map { case ((a, b)) => zip2(a, b) }

    val fabs : F[Term[AnnFAB]] = holes.builder(lunAnn, abs)

    Term[AnnFAB](Ann((lattr, rattr), fabs))
  }
}

object attr extends attr

trait phases extends attr {
  type Phase[F[_], A, B] = Attr[F, A] => Attr[F, B]

  implicit def PhaseArrow[F[_]: Functor: Foldable: Traverse] = new Arrow[({type f[a, b] = Phase[F, a, b]})#f] {
    private type AttrF[A] = Attr[F, A]

    def arr[A, B](f: A => B): Phase[F, A, B] = attr => attrMap(attr)(f)

    def first[A, B, C](f: Phase[F, A, B]): Phase[F, (A, C), (B, C)] = { (attr: Attr[F, (A, C)]) =>
      val attrA = Functor[AttrF].map(attr)(_._1)
      val attrC = Functor[AttrF].map(attr)(_._2)
      
      val attrB: Attr[F, B] = f(attrA)

      zip2(attrB, attrC)
    }

    def id[A]: Phase[F, A, A] = id

    def compose[A, B, C](f: Phase[F, B, C], g: Phase[F, A, B]): Phase[F, A, C] = f compose g
  }
}


