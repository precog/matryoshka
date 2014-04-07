package slamdata.engine.analysis

import scalaz.{Apply, Applicative, Functor, Monoid, Cofree, Foldable, Show, Cord, Tree => ZTree, Monad, Traverse, Free, Arrow, Kleisli, Zip, Comonad}

import scalaz.Tags.Disjunction

import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.function._

import scalaz.syntax.monad._

trait term {
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
}
object term extends term

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

trait zips {
  def unzipF[F[_]: Functor, A, B](f: F[(A, B)]): (F[A], F[B]) = {
    val F = Functor[F]

    (F.map(f)(_._1), F.map(f)(_._2))
  }
}
object zips extends zips

trait ann extends term with zips {
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
      type AnnFB[X] = Ann[F, B, X]

      Term[AnnFB](Ann(f(v.unFix.attr), Functor[F].map(v.unFix.unAnn)(t => AttrFunctor[F].map(t)(f))))
    }
  }

  implicit def AttrFoldable[F[_]: Foldable] = {
    type AttrF[A] = Attr[F, A]

    new Foldable[AttrF] {
      def foldMap[A, B](fa: AttrF[A])(f: A => B)(implicit F: Monoid[B]): B = {
        val head = f(fa.unFix.attr)

        val tail = Foldable[F].foldMap(fa.unFix.unAnn)(v => foldMap(v)(f))

        Monoid[B].append(head, tail)
      }

      def foldRight[A, B](fa: AttrF[A], z: => B)(f: (A, => B) => B): B = {
        f(fa.unFix.attr, Foldable[F].foldRight(fa.unFix.unAnn, z)((a, z) => foldRight(a, z)(f)))
      }
    }
  }

  implicit def AttrTraverse[F[_]: Traverse] = {
    type AttrF[A] = Attr[F, A]

    new Traverse[AttrF] {
      def traverseImpl[G[_], A, B](fa: AttrF[A])(f: A => G[B])(implicit G: Applicative[G]): G[AttrF[B]] = {
        type AnnF[X] = Ann[F, A, X]
        type AnnF2[X] = Ann[F, B, X]

        val gb: G[B] = f(fa.unFix.attr)
        val gunAnn: G[F[AttrF[B]]] = Traverse[F].traverseImpl(fa.unFix.unAnn)((v: Term[AnnF]) => traverseImpl(v)(f))

        G.apply2(gb, gunAnn)((b, unAnn) => Term[AnnF2](Ann(b, unAnn)))
      }
    }
  }

  implicit def AttrZip[F[_]: Traverse] = {
    type AttrF[A] = Attr[F, A]

    new Zip[AttrF] {
      def zip[A, B](v1: => AttrF[A], v2: => AttrF[B]): AttrF[(A, B)] = zip2(v1, v2)
    }
  }

  implicit def AttrComonad[F[_]: Functor] = {
    type AttrF[X] = Attr[F, X]


    new Comonad[AttrF] {
      def cobind[A, B](fa: AttrF[A])(f: AttrF[A] => B): AttrF[B] = {
        type AnnFB[X] = Ann[F, B, X]

        Term[AnnFB](Ann(f(fa), Functor[F].map(fa.unFix.unAnn)(term => cobind(term)(f))))
      }

      def copoint[A](p: AttrF[A]): A = p.unFix.attr

      def map[A, B](fa: AttrF[A])(f: A => B): AttrF[B] = attrMap(fa)(f)
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
    type AnnFA[X] = Ann[F, A, X]

    def loop(term: Term[F]): (Term[F], Attr[F, A]) = {
      val rec : F[(Term[F], Attr[F, A])] = Functor[F].map(term.unFix)(loop _)

      val left = Functor[F].map(rec) {
        case (s, t) => (s, attr(t))
      }

      val right = Functor[F].map(rec)(_._2)

      (term, Term[AnnFA](Ann(f(left), right)))
    }

    loop(term)._2
  }

  def synthPara2[F[_]: Functor, A](term: Term[F])(f: (Term[F], F[A]) => A): Attr[F, A] = {
    type AnnFA[X] = Ann[F, A, X]

    val rec = Functor[F].map(term.unFix)(synthPara2(_)(f))

    val fa = Functor[F].map(rec)(_.unFix.attr)

    Term[AnnFA](Ann(f(term, fa), rec))
  }

  def scanPara[F[_]: Functor, A, B](attr: Attr[F, A])(f: (Attr[F, A], F[B]) => B): Attr[F, B] = {
    type AnnFB[X] = Ann[F, B, X]

    val rec = Functor[F].map(attr.unFix.unAnn)(scanPara(_)(f))

    val fb = Functor[F].map(rec)(_.unFix.attr)

    Term[AnnFB](Ann(f(attr, fb), rec))
  }

  def zynthZygo_[F[_]: Functor, A, B](term: Term[F])(f: F[B] => B, g: F[(B, A)] => A): Attr[F, A] = {
    synthZygoWith[F, A, B, A](term)((b: B, a: A) => a, f, g)
  }

  def synthZygo[F[_]: Functor, A, B](term: Term[F])(f: F[B] => B, g: F[(B, A)] => A): Attr[F, (B, A)] = {
    synthZygoWith[F, A, B, (B, A)](term)((b: B, a: A) => (b, a), f, g)
  }

  def synthZygoWith[F[_]: Functor, A, B, C](term: Term[F])(f: (B, A) => C, g: F[B] => B, h: F[(B, A)] => A): Attr[F, C] = {
    type AnnFC[X] = Ann[F, C, X]

    def loop(term: Term[F]): ((B, A), Attr[F, C]) = {
      val (fba, s) : (F[(B, A)], F[Attr[F,C]]) = unzipF(Functor[F].map(term.unFix)(loop _))

      val b : B = g(Functor[F].map(fba)(_._1))

      val a : A = h(fba)

      val c : C = f(b, a)

      ((b, a), Term[AnnFC](Ann(c, s)))
    }
    
    loop(term)._2
  }

  def sequenceUp[F[_], G[_], A](attr: Attr[F, G[A]])(implicit F: Traverse[F], G: Applicative[G]): G[Attr[F, A]] = {
    type AnnGA[X] = Ann[F, G[A], X]
    type AnnFA[X] = Ann[F, A, X]

    val unFix = attr.unFix

    val ga : G[A] = unFix.attr
    val fgattr : F[G[Attr[F, A]]] = F.map(unFix.unAnn)(t => sequenceUp(t)(F, G))

    val gfattr : G[F[Attr[F, A]]] = F.traverseImpl(fgattr)(identity)

    G.apply2(gfattr, ga)((node, attr) => Term[AnnFA](Ann(attr, node)))
  }

  def sequenceDown[F[_], G[_], A](attr: Attr[F, G[A]])(implicit F: Traverse[F], G: Applicative[G]): G[Attr[F, A]] = {
    type AnnGA[X] = Ann[F, G[A], X]
    type AnnFA[X] = Ann[F, A, X]

    val unFix = attr.unFix

    val ga : G[A] = unFix.attr
    val fgattr : F[G[Attr[F, A]]] = F.map(unFix.unAnn)(t => sequenceDown(t)(F, G))

    val gfattr : G[F[Attr[F, A]]] = F.traverseImpl(fgattr)(identity)

    G.apply2(ga, gfattr)((attr, node) => Term[AnnFA](Ann(attr, node)))
  }

  /**
   * Zips two attributed nodes together. This is unsafe in the sense that the 
   * user is responsible to retain the shape of the node. 
   *
   * TODO: See if there's a safer, less restrictive way of doing this.
   * TODO2: Require F be Zip!!!!
   */
  def zip2[F[_]: Traverse, A, B](left: Attr[F, A], right: Attr[F, B]): Attr[F, (A, B)] = {
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

  def context[F[_]](term: Term[F])(implicit F: Traverse[F]): Attr[F, Term[F] => Term[F]] = {
    def loop(f: Term[F] => Term[F]): Attr[F, Term[F] => Term[F]] = {
      //def g(y: Term[F], replace: Term[F] => Term[F]) = loop()

      ???
    }

    loop(identity[Term[F]])
  }

  /*def zip22[F[_]: Foldable : Zip, A, B](left: Attr[F, A], right: Attr[F, B]): Attr[F, (A, B)] = {
    type AnnFA[X] = Ann[F, A, X]
    type AnnFB[X] = Ann[F, B, X]

    type AnnFAB[X] = Ann[F, (A, B), X]

    val lunFix = left.unFix

    val lattr: A = lunFix.attr
    val lunAnn: F[Term[AnnFA]] = lunFix.unAnn

    val runFix = right.unFix 
    val rattr: B = runFix.attr
    val runAnn: F[Term[AnnFB]] = runFix.unAnn

    val abs: F[Term[AnnFAB]] = lunAnnL.zip(runAnnL).map { case ((a, b)) => zip2(a, b) }

    val fabs : F[Term[AnnFAB]] = holes.builder(lunAnn, abs)

    Term[AnnFAB](Ann((lattr, rattr), fabs))
  }*/
}

object attr extends attr

trait phases extends attr {
  type Phase[F[_], A, B] = Attr[F, A] => Attr[F, B]

  implicit def PhaseArrow[F[_]: Traverse] = new Arrow[({type f[a, b] = Phase[F, a, b]})#f] {
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

  implicit class PhaseOps[F[_], A, B](self: Phase[F, A, B]) {
    def >>> [C](that: Phase[F, B, C])(implicit F: Traverse[F]) = PhaseArrow[F].compose(that, self)

    def first[C](implicit F: Traverse[F]): Phase[F, (A, C), (B, C)] = PhaseArrow[F].first(self)
  }
}

trait fixplate extends holes with phases
object fixplate extends fixplate