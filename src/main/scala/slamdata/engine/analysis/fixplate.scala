package slamdata.engine.analysis

import scalaz.{Apply, Applicative, Functor, Monoid, Cofree, Foldable, Id, Show, \/, Cord, State, Tree => ZTree, Monad, Traverse, Traverse1, Free, Arrow, Kleisli, Zip, Comonad, -\/, \/-}

import Id.Id

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

    def cata[A](f: F[A] => A)(implicit F: Functor[F]): A = f(F.map(unFix)(_.cata(f)(F)))

    def para[A](f: F[(Term[F], A)] => A)(implicit F: Functor[F]): A = f(F.map(unFix)(t => t -> t.para(f)(F)))

    def para2[A](f: (Term[F], F[A]) => A)(implicit F: Functor[F]): A = f(this, F.map(unFix)(_.para2(f)(F)))

    def paraList[A](f: (Term[F], List[A]) => A)(implicit F: Functor[F], F2: Foldable[F]): A = {
      f(this, F2.foldMap(unFix)(_.paraList(f)(F, F2) :: Nil))
    }
  }

  trait TermInstances {
    implicit def TermShow[F[_]](implicit showF: Show[F[_]], foldF: Foldable[F]) = new Show[Term[F]] {
      implicit val ShowF: Show[F[Term[F]]] = new Show[F[Term[F]]] {
        override def show(fa: F[Term[F]]): Cord = showF.show(fa)
      }
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

  def apo[F[_], A](a: A)(f: A => F[Term[F] \/ A])(implicit F: Functor[F]): Term[F] = {
    Term(F.map(f(a)) {
      case -\/(term) => term
      case \/-(a)    => apo(a)(f)
    })
  }

  def ana[F[_], A](a: A)(f: A => F[A])(implicit F: Functor[F]): Term[F] = {
    Term(F.map(f(a))(a => ana(a)(f)(F)))
  }

  def hylo[F[_], A, B](b: B)(f: F[A] => A, g: B => F[B])(implicit F: Functor[F]): A = ana(b)(g).cata(f)

  def zygo_[F[_], A, B](t: Term[F])(f: F[B] => B, g: F[(B, A)] => A)(implicit F: Functor[F]): A = zygo(t)(f, g)(F)._2

  def zygo[F[_], A, B](t: Term[F])(f: F[B] => B, g: F[(B, A)] => A)(implicit F: Functor[F]): (B, A) = {
    val fba = F.map(t.unFix)(zygo(_)(f, g)(F))

    val b = f(F.map(fba)(_._1))
    val a = g(fba)

    (b, a)
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

  def liftAnn[F[_], G[_], A, E](f: F[E] => G[E], ann: Ann[F, A, E]): Ann[G, A, E] = Ann(ann.attr, f(ann.unAnn))

  def liftCoAnn[F[_], G[_], A, E](f: F[E] => G[E], coann: CoAnn[F, A, E]): CoAnn[G, A, E] = coann match {
    case CoAnn.Pure(attr) => CoAnn.Pure(attr)
    case CoAnn.UnAnn(unAnn) => CoAnn.UnAnn(f(unAnn))
  }
}

trait attr extends ann {
  type Attr[F[_], A] = Term[({type f[b]=Ann[F, A, b]})#f]

  object Attr {
    // Helper functions to make it easier to NOT annotate constructors.

    def apply[F[_], A](a: A, f: F[Attr[F, A]]): Term[({type f[X] = Ann[F, A, X]})#f] = {
      type AnnFA[X] = Ann[F, A, X]

      Term[AnnFA](Ann(a, f))
    }

    def unapply[F[_], A](a: Attr[F, A]): Option[(A, F[Attr[F, A]])] = Some((a.unFix.attr, a.unFix.unAnn))
  }

  type CoAttr[F[_], A] = Term[({type f[b]=CoAnn[F, A, b]})#f]

  def attr[F[_], A](attr: Attr[F, A]): A = attr.unFix.attr

  def attrUnit[F[_]: Functor](term: Term[F]): Attr[F, Unit] = {
    type AnnFUnit[X] = Ann[F, Unit, X]

    Term[AnnFUnit](Ann(Unit, Functor[F].map(term.unFix)(attrUnit(_)(Functor[F]))))
  }

  def attrSelf[F[_]: Functor](term: Term[F]): Attr[F, Term[F]] = {
    type AnnFTermF[X] = Ann[F, Term[F], X]

    Term[AnnFTermF](Ann(term, Functor[F].map(term.unFix)(attrSelf(_)(Functor[F]))))
  }

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
      def zip[A, B](v1: => AttrF[A], v2: => AttrF[B]): AttrF[(A, B)] = unsafeZip2(v1, v2)
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

 def histo[F[_], A](t: Term[F])(f: F[Attr[F, A]] => A)(implicit F: Functor[F]): A = {
    type AnnFA[X] = Ann[F, A, X]

    def g: Term[F] => Attr[F, A] = { t => 
      val a = histo(t)(f)(F)

      Attr(a, F.map(t.unFix)(g))
    }

    f(F.map(t.unFix)(g))
  }

  def futu[F[_], A](a: A)(f: A => F[CoAttr[F, A]])(implicit F: Functor[F]): Term[F] = {
    def g: CoAttr[F, A] => Term[F] = t => t.unFix match {
      case CoAnn.Pure(attr)     => futu(a)(f)(F)
      case CoAnn.UnAnn(fcoattr) => Term(F.map(fcoattr)(g))
    }

    Term(F.map(f(a))(g))
  }

  def synthetize[F[_]: Functor, A](term: Term[F])(f: F[A] => A): Attr[F, A] = synthCata(term)(f)

  def synthCata[F[_]: Functor, A](term: Term[F])(f: F[A] => A): Attr[F, A] = {
    type AnnF[X] = Ann[F, A, X]

    val fattr: F[Attr[F, A]] = Functor[F].map(term.unFix)(t => synthCata(t)(f))

    val fa: F[A] = Functor[F].map(fattr)(attr _)

    Attr(f(fa), fattr)
  }

  def scanCata[F[_]: Functor, A, B](attr0: Attr[F, A])(f: (A, F[B]) => B): Attr[F, B] = {
    type AnnF[X] = Ann[F, B, X]

    val a : A = attr0.unFix.attr

    val unAnn = attr0.unFix.unAnn

    val fattr: F[Attr[F, B]] = Functor[F].map(unAnn)(t => scanCata(t)(f))

    val b : F[B] = Functor[F].map(fattr)(attr _)

    Attr(f(a, b), fattr)
  }

  def synthPara2[F[_]: Functor, A](term: Term[F])(f: F[(Term[F], A)] => A): Attr[F, A] = {
    scanPara(attrUnit(term))((_, ffab) => f(Functor[F].map(ffab) { case (tf, a, b) => (tf, b) }))
  }

  def synthPara3[F[_]: Functor, A](term: Term[F])(f: (Term[F], F[A]) => A): Attr[F, A] = {
    scanPara(attrUnit(term))((attrfa, ffab) => f(forget(attrfa), Functor[F].map(ffab)(_._3)))
  }

  def scanPara[F[_]: Functor, A, B](attr: Attr[F, A])(f: (Attr[F, A], F[(Term[F], A, B)]) => B): Attr[F, B] = {
    type AnnFAB[X] = Ann[F, (A, B), X]

    def loop(term: Attr[F, A]): (Term[F], Attr[F, (A, B)]) = {
      val rec : F[(Term[F], Attr[F, (A, B)])] = Functor[F].map(term.unFix.unAnn)(loop _)

      val left: F[(Term[F], A, B)] = Functor[F].map(rec) {
        case (tf, t) => 
          val (a, b) = t.unFix.attr

          (tf, a, b)
      }

      val right = Functor[F].map(rec)(_._2)

      val a = term.unFix.attr

      (forget(term), Attr((a, f(term, left)), right))
    }

    AttrFunctor[F].map(loop(attr)._2)(_._2)
  }

  def scanPara2[F[_]: Functor, A, B](attr: Attr[F, A])(f: (A, F[(Term[F], A, B)]) => B): Attr[F, B] = {
    scanPara(attr)((attrfa, ffab) => f(attrfa.unFix.attr, ffab))
  }

  def scanPara3[F[_]: Functor, A, B](attr: Attr[F, A])(f: (Attr[F, A], F[B]) => B): Attr[F, B] = {
    scanPara(attr)((attrfa, ffab) => f(attrfa, Functor[F].map(ffab)(_._3)))
  }

  def synthZygo_[F[_]: Functor, A, B](term: Term[F])(f: F[B] => B, g: F[(B, A)] => A): Attr[F, A] = {
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

      ((b, a), Attr(c, s))
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

    G.apply2(gfattr, ga)((node, attr) => Attr(attr, node))
  }

  def sequenceDown[F[_], G[_], A](attr: Attr[F, G[A]])(implicit F: Traverse[F], G: Applicative[G]): G[Attr[F, A]] = {
    type AnnGA[X] = Ann[F, G[A], X]
    type AnnFA[X] = Ann[F, A, X]

    val unFix = attr.unFix

    val ga : G[A] = unFix.attr
    val fgattr : F[G[Attr[F, A]]] = F.map(unFix.unAnn)(t => sequenceDown(t)(F, G))

    val gfattr : G[F[Attr[F, A]]] = F.traverseImpl(fgattr)(identity)

    G.apply2(ga, gfattr)((attr, node) => Attr(attr, node))
  }

  /**
   * Zips two attributed nodes together. This is unsafe in the sense that the 
   * user is responsible for ensuring both left and right parameters have the
   * same shape (i.e. represent the same tree).
   */
  def unsafeZip2[F[_]: Traverse, A, B](left: Attr[F, A], right: Attr[F, B]): Attr[F, (A, B)] = {
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

    val abs: List[Term[AnnFAB]] = lunAnnL.zip(runAnnL).map { case ((a, b)) => unsafeZip2(a, b) }

    val fabs : F[Term[AnnFAB]] = holes.builder(lunAnn, abs)

    Attr((lattr, rattr), fabs)
  }

  def context[F[_]](term: Term[F])(implicit F: Traverse[F]): Attr[F, Term[F] => Term[F]] = {
    def loop(f: Term[F] => Term[F]): Attr[F, Term[F] => Term[F]] = {
      //def g(y: Term[F], replace: Term[F] => Term[F]) = loop()

      ???
    }

    loop(identity[Term[F]])
  }
}

object attr extends attr

trait phases extends attr {
  /**
   * An annotation phase, represented as a monadic function from an attributed 
   * tree of one type (A) to an attributed tree of another type (B).
   *
   * This is a kleisli function, but specialized to transformations of attributed trees.
   *
   * The fact that a phase is monadic may be used to capture and propagate error
   * information. Typically, error information is produced at the level of each
   * node, but through sequenceUp / sequenceDown, the first error can be pulled 
   * out to yield a kleisli function.
   */
  case class PhaseM[M[_], F[_], A, B](value: Attr[F, A] => M[Attr[F, B]]) extends (Attr[F, A] => M[Attr[F, B]]) {
    def apply(x: Attr[F, A]) = value(x)
  }

  def liftPhase[M[_]: Monad, F[_], A, B](phase: Phase[F, A, B]): PhaseM[M, F, A, B] = {
    PhaseM(attr => Monad[M].point(phase(attr)))
  }

  /**
   * A non-monadic phase. This is only interesting for phases that cannot produce 
   * errors and don't need state.
   */
  type Phase[F[_], A, B] = PhaseM[Id, F, A, B]
  
  def Phase[F[_], A, B](x: Attr[F, A] => Attr[F, B]): Phase[F, A, B] = PhaseM[Id, F, A, B](x)

  /**
   * A phase that can produce errors. An error is captured using the left side of \/.
   */
  type PhaseE[F[_], E, A, B] = PhaseM[({type f[X] = E \/ X})#f, F, A, B]

  def PhaseE[F[_], E, A, B](x: Attr[F, A] => E \/ Attr[F, B]): PhaseE[F, E, A, B] = {
    type EitherE[X] = E \/ X

    PhaseM[EitherE, F, A, B](x)
  }

  def toPhaseE[F[_]: Traverse, E, A, B](phase: Phase[F, A, E \/ B]): PhaseE[F, E, A, B] = {
    type EitherE[X] = E \/ X

    PhaseE(attr => sequenceUp[F, EitherE, B](phase(attr)))
  }

  def liftPhaseE[F[_], E, A, B](phase: Phase[F, A, B]): PhaseE[F, E, A, B] = liftPhase[({type f[X] = E \/ X})#f, F, A, B](phase)

  /**
   * A phase that requires state. State is represented using the state monad.
   */
  type PhaseS[F[_], S, A, B] = PhaseM[({type f[X] = State[S, X]})#f, F, A, B]

  def PhaseS[F[_], S, A, B](x: Attr[F, A] => State[S, Attr[F, B]]): PhaseS[F, S, A, B] = {
    type StateS[X] = State[S, X]

    PhaseM[StateS, F, A, B](x)
  }

  def toPhaseS[F[_]: Traverse, S, A, B](phase: Phase[F, A, State[S, B]]): PhaseS[F, S, A, B] = {
    type StateS[X] = State[S, X]    

    PhaseS(attr => sequenceUp[F, StateS, B](phase(attr)))
  }  

  def liftPhaseS[F[_], S, A, B](phase: Phase[F, A, B]): PhaseS[F, S, A, B] = liftPhase[({type f[X] = State[S, X]})#f, F, A, B](phase)

  implicit def PhaseMArrow[M[_], F[_]](implicit F: Traverse[F], M: Monad[M]) = new Arrow[({type f[a, b] = PhaseM[M, F, a, b]})#f] {
    type Arr[A, B] = PhaseM[M, F, A, B]
    type AttrF[X] = Attr[F, X]

    def arr[A, B](f: A => B): Arr[A, B] = PhaseM(attr => M.point(attrMap(attr)(f)))
    
    def first[A, B, C](f: Arr[A, B]): Arr[(A, C), (B, C)] = PhaseM { (attr: Attr[F, (A, C)]) =>
      val attrA = Functor[AttrF].map(attr)(_._1)
      val mattrC = M.point(Functor[AttrF].map(attr)(_._2))
      
      val mattrB: M[Attr[F, B]] = f(attrA)

      for {
        b <- mattrB
        c <- mattrC        
      } yield unsafeZip2(b, c)
    }

    def id[A]: Arr[A, A] = PhaseM(attr => M.point(attr))
      
    def compose[A, B, C](f: Arr[B, C], g: Arr[A, B]): Arr[A, C] = PhaseM { (attr: Attr[F, A]) =>
      for {
        b <- g(attr)
        a <- f(b)
      } yield a
    }
  }

  implicit class ToPhaseOps[M[_]: Monad, F[_]: Traverse, A, B](self: PhaseM[M, F, A, B]) {
    def >>> [C](that: PhaseM[M, F, B, C]) = PhaseMArrow[M, F].compose(that, self)

    def first[C]: PhaseM[M, F, (A, C), (B, C)] = PhaseMArrow[M, F].first(self)

    def second[C]: PhaseM[M, F, (C, A), (C, B)] = PhaseM { (attr: Attr[F, (C, A)]) => 
      first.map((t: (B, C)) => (t._2, t._1))(attrMap(attr)((t: (C, A)) => (t._2, t._1)))
    }

    def map[C](f: B => C): PhaseM[M, F, A, C] = PhaseM((attr: Attr[F, A]) => Functor[M].map(self(attr))(attrMap(_)(f)))

    def dup: PhaseM[M, F, A, (B, B)] = map(v => (v, v))

    def fork[C, D](left: PhaseM[M, F, B, C], right: PhaseM[M, F, B, D]): PhaseM[M, F, A, (C, D)] = PhaseM { (attr: Attr[F, A]) =>
      (dup >>> (left.first) >>> (right.second))(attr)
    }
  }

  implicit class ToPhaseEOps[F[_]: Traverse, E, A, B](self: PhaseE[F, E, A, B]) {
    // This abomination exists because Scala has no higher-kinded type inference and I
    // can't figure out how to make ToPhaseOps work for PhaseE (despite the fact that
    // PhaseE is just a type synonym for PhaseM). Revisit later.
    type M[X] = E \/ X

    val ops = ToPhaseOps[M, F, A, B](self)

    def >>> [C](that: PhaseE[F, E, B, C]) = ops >>> that

    def first[C]: PhaseE[F, E, (A, C), (B, C)] = ops.first[C]

    def second[C]: PhaseE[F, E, (C, A), (C, B)] = ops.second[C]

    def map[C](f: B => C): PhaseE[F, E, A, C] = ops.map[C](f)

    def dup: PhaseE[F, E, A, (B, B)] = ops.dup

    def fork[C, D](left: PhaseE[F, E, B, C], right: PhaseE[F, E, B, D]): PhaseE[F, E, A, (C, D)] = ops.fork[C, D](left, right)
  }
}

trait fixplate extends holes with phases
object fixplate extends fixplate