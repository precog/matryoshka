package slamdata.engine.analysis

import slamdata.engine.fp._

import scalaz.{Tree => ZTree, Node => _, _}
import Scalaz._

import Id.Id

import slamdata.engine.{RenderTree, Terminal, NonTerminal}

sealed trait term {
  case class Term[F[_]](unFix: F[Term[F]]) {
    def cofree(implicit f: Functor[F]): Cofree[F, Unit] =
      Cofree(Unit, Functor[F].map(unFix)(_.cofree))
    
    def isLeaf(implicit F: Foldable[F]): Boolean =
      Tag.unwrap(F.foldMap(unFix)(Function.const(Tags.Disjunction(true))))
    
    def children(implicit F: Foldable[F]): List[Term[F]] =
      F.foldMap(unFix)(_ :: Nil)
    
    def universe(implicit F: Foldable[F]): List[Term[F]] =
      children.flatMap(_.universe)

    def transform(f: Term[F] => Term[F])(implicit T: Traverse[F]): Term[F] =
      transformM[Free.Trampoline]((v: Term[F]) => f(v).pure[Free.Trampoline]).run

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

    def topDownTransformM[M[_]](f: Term[F] => M[Term[F]])(implicit M: Monad[M], F: Traverse[F]): M[Term[F]] = {
      def loop(term: Term[F]): M[Term[F]] = {
        for {
          x <- f(term)
          y <- F.traverse(x.unFix)(loop _)
        } yield Term(y)
      }

      loop(this)
    }

    def topDownCata[A](a: A)(f: (A, Term[F]) => (A, Term[F]))(implicit F: Traverse[F]): Term[F] = {
      topDownCataM[Free.Trampoline, A](a)((a: A, term: Term[F]) => f(a, term).pure[Free.Trampoline]).run
    }

    def foldMap[Z](f: Term[F] => Z)(implicit F: Traverse[F], Z: Monoid[Z]): Z = {
      (foldMapM[Free.Trampoline, Z] { (term: Term[F]) =>
        f(term).pure[Free.Trampoline]
      }).run
    }

    def foldMapM[M[_], Z](f: Term[F] => M[Z])(implicit F: Traverse[F], M: Monad[M], Z: Monoid[Z]): M[Z] = {
      def loop(z0: Z, term: Term[F]): M[Z] = {
        for {
          z1 <- f(term)
          z2 <- F.foldLeftM(term.unFix, Z.append(z0, z1))(loop(_, _))
        } yield z2
      }

      loop(Z.zero, this)
    }

    def topDownCataM[M[_], A](a: A)(f: (A, Term[F]) => M[(A, Term[F])])(implicit M: Monad[M], F: Traverse[F]): M[Term[F]] = {
      def loop(a: A, term: Term[F]): M[Term[F]] = {
        for {
          tuple <- f(a, term)

          (a, tf) = tuple

          rec   <- F.traverse(tf.unFix)(loop(a, _))
        } yield Term(rec)
      }

      loop(a, this)
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

    def trans[G[_]](f: F ~> G)(implicit G: Functor[G]): Term[G] = Term[G](G.map(f(unFix))(_.trans(f)(G)))

    def cata[A](f: F[A] => A)(implicit F: Functor[F]): A = f(F.map(unFix)(_.cata(f)(F)))

    def para[A](f: F[(Term[F], A)] => A)(implicit F: Functor[F]): A = f(F.map(unFix)(t => t -> t.para(f)(F)))

    def para2[A](f: (Term[F], F[A]) => A)(implicit F: Functor[F]): A = f(this, F.map(unFix)(_.para2(f)(F)))

    def paraList[A](f: (Term[F], List[A]) => A)(implicit F: Functor[F], F2: Foldable[F]): A = {
      f(this, F2.foldMap(unFix)(_.paraList(f)(F, F2) :: Nil))
    }

    override def toString = unFix.toString
  }

  sealed trait TermInstances {
    implicit def TermRenderTree[F[_]](implicit F: Foldable[F], RF: RenderTree[F[_]]) = new RenderTree[Term[F]] {
      override def render(v: Term[F]) = {
        val t = RF.render(v.unFix)
        NonTerminal(t.label, v.children.map(render(_)), t.nodeType)
      }
    }
    implicit def TermEqual[F[_]](implicit equalF: EqualF[F]): Equal[Term[F]] = new Equal[Term[F]] {
      implicit val EqualFTermF = new Equal[F[Term[F]]] {
        def equal(v1: F[Term[F]], v2: F[Term[F]]): Boolean = {
          equalF.equal(v1, v2)(TermEqual[F](equalF))
        }
      }

      def equal(v1: Term[F], v2: Term[F]): Boolean = {
        EqualFTermF.equal(v1.unFix, v2.unFix)
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

sealed trait holes {
  sealed trait Hole
  val Hole = new Hole{}

  def holes[F[_]: Traverse, A](fa: F[A]): F[(A, A => F[A])] = holes2(fa)(identity)

  def holes2[F[_], A, B](fa: F[A])(f: A => B)(implicit F: Traverse[F]): F[(A, A => F[B])] = {
    (F.mapAccumL(fa, 0) {
      case (i, x) =>
        val h: A => F[B] = { y =>
          val g: (Int, A) => (Int, A) = (j, z) => (j + 1, if (i == j) y else z)

          F.map(F.mapAccumL(fa, 0)(g)._2)(f)
        }

        (i + 1, (x, h))
    })._2
  }

  def holesList[F[_]: Traverse, A](fa: F[A]): List[(A, A => F[A])] = Traverse[F].toList(holes(fa))

  def transformChildren[F[_]: Traverse, A](fa: F[A])(f: A => A): F[F[A]] = {
    val g: (A, A => F[A]) => F[A] = (x, replace) => replace(f(x))

    Traverse[F].map(holes(fa))(g.tupled)
  }

  def transformChildren2[F[_]: Traverse, A, B](fa: F[A])(f: A => B): F[F[B]] = {
    val g: (A, A => F[B]) => F[B] = (x, replace) => replace(x)

    Traverse[F].map(holes2(fa)(f))(g.tupled)
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

sealed trait zips {
  def unzipF[F[_]: Functor, A, B](f: F[(A, B)]): (F[A], F[B]) = {
    val F = Functor[F]

    (F.map(f)(_._1), F.map(f)(_._2))
  }
}

sealed trait ann extends term with zips {
  case class Ann[F[_], A, B](attr: A, unAnn: F[B]) { ann =>
    def trans[G[_]](f: F ~> G): Ann[G, A, B] = Ann(ann.attr, f(ann.unAnn))
  }

  sealed trait CoAnn[F[_], A, B] { coann =>
    def trans[G[_]](f: F ~> G): CoAnn[G, A, B] = coann match {
      case CoAnn.Pure(attr) => CoAnn.Pure(attr)
      case CoAnn.UnAnn(unAnn) => CoAnn.UnAnn(f(unAnn))
    }
  }
  object CoAnn {
    case class Pure[F[_], A, B](attr: A) extends CoAnn[F, A, B]
    case class UnAnn[F[_], A, B](unAnn: F[B]) extends CoAnn[F, A, B]
  }

  implicit def AnnShow[F[_], A](implicit S: Show[F[_]], A: Show[A]): Show[Ann[F, A, _]] = new Show[Ann[F, A, _]] {
    override def show(v: Ann[F, A, _]): Cord = Cord("(" + A.show(v.attr) + ", " + S.show(v.unAnn) + ")")
  }
  implicit def AnnFoldable[F[_], A](implicit F: Foldable[F]): Foldable[({type f[X]=Ann[F, A, X]})#f] = new Foldable[({type f[X]=Ann[F, A, X]})#f] {
    type AnnFA[X] = Ann[F, A, X]

    def foldMap[A, B](fa: AnnFA[A])(f: A => B)(implicit B: Monoid[B]): B = F.foldMap(fa.unAnn)(f)

    def foldRight[A, B](fa: AnnFA[A], z: => B)(f: (A, => B) => B): B = F.foldRight(fa.unAnn, z)(f)
  }

  // F: scalaz.Foldable[[b]attr.this.Ann[F,A,b]]
}

sealed trait attr extends ann with holes {
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

  def attrUnit[F[_]: Functor](term: Term[F]): Attr[F, Unit] = attrK(term, ())

  def attrK[F[_]: Functor, A](term: Term[F], k: A): Attr[F, A] = {
    Attr(k, Functor[F].map(term.unFix)(attrK(_, k)(Functor[F])))
  }

  def attrSelf[F[_]: Functor](term: Term[F]): Attr[F, Term[F]] = {
    type AnnFTermF[X] = Ann[F, Term[F], X]

    Term[AnnFTermF](Ann(term, Functor[F].map(term.unFix)(attrSelf(_)(Functor[F]))))
  }

  def forget[F[_], A](attr: Attr[F, A])(implicit F: Functor[F]): Term[F] = Term(F.map(attr.unFix.unAnn)(forget[F, A](_)))

  // TODO: Do the low-priority, high-priority implicits thing to select for most powerful of
  //       functor, foldable, traverse
  def AttrFunctor[F[_]: Functor]: Functor[({type f[a]=Attr[F, a]})#f] = new Functor[({type f[a]=Attr[F, a]})#f] {
    def map[A, B](v: Attr[F, A])(f: A => B): Attr[F, B] = {
      type AnnFB[X] = Ann[F, B, X]

      Attr[F, B](f(v.unFix.attr), Functor[F].map(v.unFix.unAnn)(t => AttrFunctor[F].map(t)(f)))
    }
  }

  def AttrFoldable[F[_]: Foldable] = {
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

  implicit def AttrTraverse[F[_]: Traverse]: Traverse[({type f[X] = Attr[F,X]})#f] = {
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

  implicit def AttrRenderTree[F[_], A](implicit F: Foldable[F], RF: RenderTree[F[_]], RA: RenderTree[A]) = new RenderTree[Attr[F, A]] {
    override def render(attr: Attr[F, A]) = {
      val term = RF.render(attr.unFix.unAnn)
      val ann = RA.render(attr.unFix.attr)
      NonTerminal(term.label,
        (if (ann.children.isEmpty) NonTerminal("", ann :: Nil, List("Annotation")) else ann.copy(label="", nodeType=List("Annotation"))) ::
          attr.children.map(render(_)),
        term.nodeType)
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

  def attrMap2[F[_], A, B](attr: Attr[F, A])(f: Attr[F, A] => B)(implicit F: Functor[F]): Attr[F, B] = {
    val b = f(attr)

    Attr[F, B](b, F.map(attr.unFix.unAnn)(attrMap2(_)(f)(F)))
  }

  def duplicate[F[_]: Functor, A](attrfa: Attr[F, A]): Attr[F, Attr[F, A]] = attrMap2(attrfa)(identity)

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

  def scanPara0[F[_], A, B](term: Attr[F, A])(f: (Attr[F, A], F[Attr[F, (A, B)]]) => B)(implicit F: Functor[F]): Attr[F, B] = {
    type AnnFAB[X] = Ann[F, (A, B), X]

    def loop(term: Attr[F, A]): Attr[F, (A, B)] = {
      val rec: F[Attr[F, (A, B)]] = F.map(term.unFix.unAnn)(loop _)

      val a = term.unFix.attr
      val b = f(term, rec)

      Attr((a, b), rec)
    }

    AttrFunctor[F].map(loop(term))(_._2)
  }

  def scanPara[F[_], A, B](attr: Attr[F, A])(f: (Attr[F, A], F[(Term[F], A, B)]) => B)(implicit F: Functor[F]): Attr[F, B] = {
    scanPara0[F, A, B](attr) {
      case (attrfa, fattrfab) => 
        val ftermab = F.map(fattrfab) { (attrfab: Attr[F, (A, B)]) =>
          val (a, b) = attrfab.unFix.attr

          (forget(attrfab), a, b)
        }

        f(attrfa, ftermab)
    }
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

  // synthAccumCata, synthAccumPara2, mapAccumCata, synthCataM, synthParaM, synthParaM2
  
  // Inherited: inherit, inherit2, inherit3, inheritM, inheritM_
  def inherit[F[_], A, B](tree: Attr[F, A], b: B)(f: (B, Attr[F, A]) => B)(implicit F: Functor[F]): Attr[F, B] = {  
    val b2 = f(b, tree)

    Attr[F, B](b2, F.map(tree.unFix.unAnn)(inherit(_, b2)(f)(F)))
  }

  // TODO: Top down folds

  def transform[F[_], A](attrfa: Attr[F, A])(f: A => Option[Attr[F, A]])(implicit F: Functor[F]): Attr[F, A] = {
    lazy val fattrfa = F.map(attrfa.unFix.unAnn)(transform(_)(f)(F))

    val a = attrfa.unFix.attr

    f(a).map(transform(_)(f)(F)).getOrElse(Attr(a, fattrfa))
  }

  def swapTransform[F[_], A, B](attrfa: Attr[F, A])(f: A => B \/ Attr[F, B])(implicit F: Functor[F]): Attr[F, B] = {
    lazy val fattrfb = F.map(attrfa.unFix.unAnn)(swapTransform(_)(f)(F))

    val a = attrfa.unFix.attr

    f(a).fold(Attr(_, fattrfb), identity)
  }


  // Questionable value...
  def circulate[F[_], A, B](tree: Attr[F, A])(f: A => B, up: (B, B) => B, down: (B, B) => B)(implicit F: Traverse[F]): Attr[F, B] = {
    val pullup: Attr[F, B] = scanPara[F, A, B](tree) { (attr: Attr[F, A], fa: F[(Term[F], A, B)]) => 
      F.foldLeft(fa, f(attr.unFix.attr))((acc, t) => up(up(f(t._2), t._3), acc))
    }

    def pushdown(attr: Attr[F, B]): Attr[F, B] = {
      val b1 = attr.unFix.attr

      Attr[F, B](b1, F.map(attr.unFix.unAnn) { attr =>
        val b2 = attr.unFix.attr

        val b3 = down(b1, b2)

        pushdown(Attr[F, B](b3, attr.unFix.unAnn))
      })
    }

    pushdown(pullup)
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

    val fabs : F[Term[AnnFAB]] = builder(lunAnn, abs)

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

sealed trait phases extends attr {
  /**
   * An annotation phase, represented as a monadic function from an attributed 
   * tree of one type (A) to an attributed tree of another type (B).
   *
   * This is a kleisli function, but specialized to transformations of
   * attributed trees.
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

      (f(attrA) |@| M.point(Functor[AttrF].map(attr)(_._2)))(unsafeZip2(_, _))
    }

    def id[A]: Arr[A, A] = PhaseM(attr => M.point(attr))
      
    def compose[A, B, C](f: Arr[B, C], g: Arr[A, B]): Arr[A, C] =
      PhaseM { (attr: Attr[F, A]) => g(attr).flatMap(f) }
  }

  implicit class ToPhaseMOps[M[_]: Monad, F[_]: Traverse, A, B](self: PhaseM[M, F, A, B]) {
    def >>> [C](that: PhaseM[M, F, B, C]) = PhaseMArrow[M, F].compose(that, self)

    def &&& [C](that: PhaseM[M, F, A, C]) = PhaseMArrow[M, F].combine(self, that)
    def *** [C, D](that: PhaseM[M, F, C, D]) = PhaseMArrow[M, F].split(self, that)

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

  implicit class ToPhaseSOps[F[_]: Traverse, S, A, B](self: PhaseS[F, S, A, B]) {
    // This abomination exists because Scala has no higher-kinded type inference
    // and I can't figure out how to make ToPhaseMOps work for PhaseE (despite
    // the fact that PhaseE is just a type synonym for PhaseM). Revisit later.
    type M[X] = State[S, X]

    val ops = ToPhaseMOps[M, F, A, B](self)

    def >>> [C](that: PhaseS[F, S, B, C])    = ops >>> that
    def &&& [C](that: PhaseS[F, S, A, C])    = ops &&& that
    def *** [C, D](that: PhaseS[F, S, C, D]) = ops *** that

    def first[C]: PhaseS[F, S, (A, C), (B, C)] = ops.first[C]

    def second[C]: PhaseS[F, S, (C, A), (C, B)] = ops.second[C]

    def map[C](f: B => C): PhaseS[F, S, A, C] = ops.map[C](f)

    def dup: PhaseS[F, S, A, (B, B)] = ops.dup

    def fork[C, D](left: PhaseS[F, S, B, C], right: PhaseS[F, S, B, D]): PhaseS[F, S, A, (C, D)] = ops.fork[C, D](left, right)
  }

  implicit class ToPhaseEOps[F[_]: Traverse, E, A, B](self: PhaseE[F, E, A, B]) {
    // This abomination exists because Scala has no higher-kinded type inference
    // and I can't figure out how to make ToPhaseMOps work for PhaseE (despite
    // the fact that PhaseE is just a type synonym for PhaseM). Revisit later.
    type M[X] = E \/ X

    val ops = ToPhaseMOps[M, F, A, B](self)

    def >>> [C](that: PhaseE[F, E, B, C])    = ops >>> that
    def &&& [C](that: PhaseE[F, E, A, C])    = ops &&& that
    def *** [C, D](that: PhaseE[F, E, C, D]) = ops *** that

    def first[C]: PhaseE[F, E, (A, C), (B, C)] = ops.first[C]

    def second[C]: PhaseE[F, E, (C, A), (C, B)] = ops.second[C]

    def map[C](f: B => C): PhaseE[F, E, A, C] = ops.map[C](f)

    def dup: PhaseE[F, E, A, (B, B)] = ops.dup

    def fork[C, D](left: PhaseE[F, E, B, C], right: PhaseE[F, E, B, D]): PhaseE[F, E, A, (C, D)] = ops.fork[C, D](left, right)
  }

  implicit class ToPhaseOps[F[_]: Traverse, A, B](self: Phase[F, A, B]) {
    // This abomination exists because Scala has no higher-kinded type inference
    // and I can't figure out how to make ToPhaseMOps work for Phase (despite
    // the fact that Phase is just a type synonym for PhaseM). Revisit later.
    val ops = ToPhaseMOps[Id, F, A, B](self)

    def >>> [C](that: Phase[F, B, C])    = ops >>> that
    def &&& [C](that: Phase[F, A, C])    = ops &&& that
    def *** [C, D](that: Phase[F, C, D]) = ops *** that

    def first[C]: Phase[F, (A, C), (B, C)] = ops.first[C]

    def second[C]: Phase[F, (C, A), (C, B)] = ops.second[C]

    def map[C](f: B => C): Phase[F, A, C] = ops.map[C](f)

    def dup: Phase[F, A, (B, B)] = ops.dup

    def fork[C, D](left: Phase[F, B, C], right: Phase[F, B, D]): Phase[F, A, (C, D)] = ops.fork[C, D](left, right)
  }
}

sealed trait binding extends phases {
  trait Binder[F[_], G[_]] {
    type AttrF[A] = Attr[F, A]

    // The combination of an attributed node and the bindings valid in this scope.
    type `AttrF * G`[A] = (AttrF[A], G[A])

    // A function that can lift an attribute into an attributed node.
    type Unsubst[A] = A => AttrF[A]

    type Subst[A] = Option[(AttrF[A], Forall[Unsubst])]

    // Extracts bindings from a node:
    val bindings: AttrF ~> G

    // Possibly binds a free term to its definition:
    val subst: `AttrF * G` ~> Subst

    def apply[M[_], A, B](phase: PhaseM[M, F, A, B])(implicit F: Traverse[F], G: Monoid[G[A]], M: Functor[M]): PhaseM[M, F, A, B] = PhaseM[M, F, A, B] { attrfa =>
      def subst0(ga0: G[A], attrfa: AttrF[A]): AttrF[(A, Option[Forall[Unsubst]])] = {
        // Possibly swap out this node for another node:
        val optT: Option[(AttrF[A], Forall[Unsubst])] = subst((attrfa, ga0))

        val (attrfa2, optF) = optT.map(tuple => tuple._1 -> Some(tuple._2)).getOrElse(attrfa -> None)

        // Add any new bindings:
        val ga: G[A] = G.append(ga0, bindings(attrfa2))

        val Ann(a, node) = attrfa2.unFix

        // Recursively apply binding:
        Attr[F, (A, Option[Forall[Unsubst]])](a -> optF, F.map(node)(subst0(ga, _)))
      }

      val attrft: AttrF[(A, Option[Forall[Unsubst]])] = subst0(G.zero, attrfa)

      val mattrfb: M[AttrF[B]] = phase(attrMap(attrft)(_._1))

      M.map(mattrfb) { attrfb =>
        val zipped = unsafeZip2(attrfb, attrMap(attrft)(_._2))

        swapTransform(zipped) {
          case (b, None) => -\/ (b)
          case (b, Some(f)) => \/- (f[B](b))
        }
      }
    }
  }

  def bound[M[_], F[_], G[_], A, B](phase: PhaseM[M, F, A, B])(implicit M: Functor[M], F: Traverse[F], G: Monoid[G[A]], B: Binder[F, G]): PhaseM[M, F, A, B] = {
    B.apply[M, A, B](phase)
  }

  def boundE[F[_], E, G[_], A, B](phase: PhaseE[F, E, A, B])(implicit F: Traverse[F], G: Monoid[G[A]], B: Binder[F, G]): PhaseE[F, E, A, B] = {
    type EitherE[A] = E \/ A

    B.apply[EitherE, A, B](phase)
  }
}

object fixplate extends binding
