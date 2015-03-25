package slamdata.engine.analysis

import slamdata.engine.fp._

import scalaz.{Tree => ZTree, Node => _, _}
import Scalaz._

import Id.Id

import slamdata.engine.{RenderTree, Terminal, NonTerminal}

sealed trait term {
  case class Term[F[_]](unFix: F[Term[F]]) {
    def isLeaf(implicit F: Foldable[F]): Boolean =
      !Tag.unwrap(F.foldMap(unFix)(κ(Tags.Disjunction(true))))

    def children(implicit F: Foldable[F]): List[Term[F]] =
      F.foldMap(unFix)(_ :: Nil)

    def universe(implicit F: Foldable[F]): List[Term[F]] =
      this :: children.flatMap(_.universe)

    def transform(f: Term[F] => Term[F])(implicit T: Traverse[F]): Term[F] =
      transformM[Free.Trampoline]((v: Term[F]) => f(v).pure[Free.Trampoline]).run

    def transformM[M[_]](f: Term[F] => M[Term[F]])(implicit M: Monad[M], F: Traverse[F]): M[Term[F]] = {
      def loop(term: Term[F]): M[Term[F]] = {
        for {
          y <- F.traverse(term.unFix)(loop _)
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

    def cata[A](f: F[A] => A)(implicit F: Functor[F]): A =
      f(unFix.map(_.cata(f)(F)))

    def cataM[M[_]: Monad, A](f: F[A] => M[A])(implicit F: Traverse[F]): M[A] =
      unFix.traverse(_.cataM(f)).flatMap(f)

    def para[A](f: F[(Term[F], A)] => A)(implicit F: Functor[F]): A =
      f(unFix.map(t => t -> t.para(f)(F)))

    def gpara[W[_]: Comonad, A](
      t: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ,
      f: F[EnvT[Term[F], W, A]] => A)(implicit F: Functor[F]):
        A =
      gzygo[W, A, Term[F]](Term(_), t, f)

    def gcata[W[_]: Comonad, A](
      k: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ,
      g: F[W[A]] => A)(
      implicit F: Functor[F]):
        A = {
      def loop(t: Term[F]): W[F[W[A]]] = k(t.unFix.map(loop(_).map(g).cojoin))

      g(loop(this).copoint)
    }

    def zygo[A, B](f: F[B] => B, g: F[(B, A)] => A)(implicit F: Functor[F]): A =
      gcata[({ type λ[α] = (B, α) })#λ, A](distZygo(f), g)

    def gzygo[W[_], A, B](
      f: F[B] => B,
      w: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ,
      g: F[EnvT[B, W, A]] => A)(
      implicit F: Functor[F], W: Comonad[W]):
        A =
      gcata[({ type λ[α] = EnvT[B, W, α] })#λ, A](distZygoT(f, w), g)

    def histo[A](f: F[Cofree[F, A]] => A)(implicit F: Functor[F]): A =
      gcata[({ type λ[α] = Cofree[F, α] })#λ, A](distHisto, f)

    def ghisto[H[_]: Functor, A](
      g: ({ type λ[α] = F[H[α]] })#λ ~> ({ type λ[α] = H[F[α]] })#λ,
      f: F[Cofree[H, A]] => A)(implicit F: Functor[F]):
        A =
      gcata[({ type λ[α] = Cofree[H, α] })#λ, A](distGHisto(g), f)

    // There is probably a standard thing for this, but I don’t know what
    def applyNT[G[_]](nt: F ~> G)(implicit F: Functor[F]): Term[G] =
      Term(nt(unFix.map(_.applyNT(nt))))

    def paraZygo[A, B](
      f: F[(Term[F], B)] => B, g: F[(B, A)] => A)(
      implicit F: Functor[F], U: Unzip[F]):
        A = {
      def h(t: Term[F]): (B, A) =
        (t.unFix.map { x =>
          val (b, a) = h(x)
          ((x, b), (b, a))
        }).unfzip.bimap(f, g)

      h(this)._2
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

    implicit def TermEqual[F[_]](implicit F: Equal ~> ({type λ[α] = Equal[F[α]]})#λ):
        Equal[Term[F]] =
      Equal.equal { (a, b) => F(TermEqual[F]).equal(a.unFix, b.unFix) }
  }
  object Term extends TermInstances

  def distPara[F[_]: Functor]:
      ({ type λ[α] = F[(Term[F], α)] })#λ ~> ({ type λ[α] = (Term[F], F[α]) })#λ =
    distZygo(Term(_))

  def distParaT[F[_]: Functor, W[_]: Comonad](
    t: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ):
      (({ type λ[α] = F[EnvT[Term[F], W, α]] })#λ ~> ({ type λ[α] = EnvT[Term[F], W, F[α]] })#λ) =
    distZygoT(Term(_), t)

  def distCata[F[_]]:
      ({ type λ[α] = F[Id[α]] })#λ ~> ({ type λ[α] = Id[F[α]] })#λ =
    NaturalTransformation.refl

  def distZygo[F[_]: Functor, B](g: F[B] => B) =
    new (({ type λ[α] = F[(B, α)] })#λ ~> ({ type λ[α] = (B,  F[α]) })#λ) {
      def apply[α](m: F[(B, α)]) = (g(m.map(_._1)), m.map(_._2))
    }

  def distZygoT[F[_], W[_], B](
    g: F[B] => B,
    k: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ)(
    implicit F: Functor[F], W: Comonad[W]) =
    new (({ type λ[α] = F[EnvT[B, W, α]] })#λ ~> ({ type λ[α] = EnvT[B, W, F[α]] })#λ) {
      def apply[α](fe: F[EnvT[B, W, α]]) =
        EnvT((
          g(F.lift[EnvT[B, W, α], B](_.ask)(fe)),
          k(F.lift[EnvT[B, W, α], W[α]](_.lower)(fe))))
    }

  def distHisto[F[_]: Functor] =
    new (({ type λ[α] = F[Cofree[F, α]] })#λ ~> ({ type λ[α] = Cofree[F, F[α]] })#λ) {
      def apply[α](m: F[Cofree[F, α]]) =
        distGHisto[F, F](NaturalTransformation.refl[({ type λ[α] = F[F[α]] })#λ]).apply(m)
    }

  def distGHisto[F[_],  H[_]](
    k: ({ type λ[α] = F[H[α]] })#λ ~> ({ type λ[α] = H[F[α]] })#λ)(
    implicit F: Functor[F], H: Functor[H]) =
    new (({ type λ[α] = F[Cofree[H, α]] })#λ ~> ({ type λ[α] = Cofree[H, F[α]] })#λ) {
      def apply[α](m: F[Cofree[H, α]]) =
        Cofree.unfold(m)(as => (
          F.lift[Cofree[H, α], α](_.copure)(as),
          k(F.lift[Cofree[H, α], H[Cofree[H, α]]](_.tail)(as))))
    }

  def ana[F[_]: Functor, A](a: A)(f: A => F[A]): Term[F] =
    Term(f(a).map(ana(_)(f)))

  def anaM[F[_]: Traverse, M[_]: Monad, A](a: A)(f: A => M[F[A]]): M[Term[F]] =
    f(a).flatMap(_.traverse(anaM(_)(f))).map(Term(_))

  def apo[F[_]: Functor, A](a: A)(f: A => F[Term[F] \/ A]): Term[F] =
    Term(f(a).map(_.fold(ɩ, apo(_)(f))))

  def postpro[F[_]: Functor, A](a: A)(e: F ~> F, g: A => F[A]): Term[F] =
    Term(g(a).map(x => ana(postpro(x)(e, g))(x => e(x.unFix))))

  def gpostpro[F[_]: Functor, M[_], A](
    a: A)(
    k: ({ type λ[α] = M[F[α]] })#λ ~> ({ type λ[α] = F[M[α]] })#λ,
    e: F ~> F,
    g: A => F[M[A]])(
    implicit M: Monad[M]):
      Term[F] = {
    def loop(ma: M[A]): Term[F] =
      Term(k(M.lift(g)(ma)).map(x => ana(loop(x.join))(x => e(x.unFix))))

    loop(a.point[M])
  }

  def hylo[F[_]: Functor, A, B](a: A)(f: F[B] => B, g: A => F[A]): B =
    f(g(a).map(hylo(_)(f, g)))

  def gana[M[_], F[_]: Functor, A](
    a: A)(
    k: ({ type λ[α] = M[F[α]] })#λ ~> ({ type λ[α] = F[M[α]] })#λ,
    f: A => F[M[A]])(
    implicit M: Monad[M]):
      Term[F] = {
    def loop(x: M[F[M[A]]]): Term[F] =
      Term(k(x).map(x => loop(M.lift(f)(x.join))))

    loop(M.point(f(a)))
  }

  def distAna[F[_]: Functor, A]:
      ({ type λ[α] = Id[F[α]] })#λ ~> ({ type λ[α] = F[Id[α]] })#λ =
    NaturalTransformation.refl

  def ghylo[W[_]: Comonad, F[_]: Functor, M[_], A, B](
    a: A)(
    w: ({ type λ[α] = F[W[α]] })#λ ~> ({ type λ[α] = W[F[α]] })#λ,
    m: ({ type λ[α] = M[F[α]] })#λ ~> ({ type λ[α] = F[M[α]] })#λ,
    f: F[W[B]] => B,
    g: A => F[M[A]])(
    implicit M: Monad[M]):
      B = {
    def h(x: M[A]): W[B] =
      w(m(M.lift(g)(x)).map(y => h(y.join).cojoin)).map(f)

    h(a.point[M]).copoint
  }

  def futu[F[_], A](a: A)(f: A => F[Free[F, A]])(implicit F: Functor[F]):
      Term[F] =
    gana[({ type λ[α] = Free[F, α] })#λ, F, A](a)(distFutu, f)

  def futuM[F[_]: Traverse, M[_]: Monad, A](a: A)(f: A => M[F[Free[F, A]]]):
      M[Term[F]] =
    f(a).flatMap(_.traverse(futuM(_)(f))).map(Term(_))

  def distFutu[F[_]: Functor] =
    new (({ type λ[α] = Free[F, F[α]] })#λ ~> ({ type λ[α] = F[Free[F, α]] })#λ) {
      def apply[α](m: Free[F, F[α]]) =
        distGFutu[F, F](NaturalTransformation.refl[({ type λ[α] = F[F[α]] })#λ]).apply(m)
    }

  def distGFutu[H[_], F[_]](
    k: ({ type λ[α] = H[F[α]] })#λ ~> ({ type λ[α] = F[H[α]] })#λ)(
    implicit H: Functor[H], F: Functor[F]):
      (({ type λ[α] = Free[H, F[α]] })#λ ~> ({ type λ[α] = F[Free[H, α]] })#λ) =
    new (({ type λ[α] = Free[H, F[α]] })#λ ~> ({ type λ[α] = F[Free[H, α]] })#λ) {
      def apply[α](m: Free[H, F[α]]) =
        m.resume.fold(
          as => F.lift(Free.liftF(_: H[Free[H, α]]).join)(k(H.lift(distGFutu(k)(H, F)(_: Free[H, F[α]]))(as))),
          F.lift(Free.point[H, α](_)))
    }

  def chrono[F[_]: Functor, A, B](
    a: A)(
    g: F[Cofree[F, B]] => B,
    f: A => F[Free[F, A]]):
      B =
    ghylo[({ type λ[α] = Cofree[F, α] })#λ, F, ({ type λ[α] = Free[F, α] })#λ, A, B](a)(distHisto, distFutu, g, f)
}

sealed trait holes {
  sealed trait Hole
  val Hole = new Hole{}

  def holes[F[_], A](fa: F[A])(implicit F: Traverse[F]): F[(A, A => F[A])] = {
    (F.mapAccumL(fa, 0) {
      case (i, x) =>
        val h: A => F[A] = { y =>
          val g: (Int, A) => (Int, A) = (j, z) => (j + 1, if (i == j) y else z)

          F.mapAccumL(fa, 0)(g)._2
        }

        (i + 1, (x, h))
    })._2
  }

  def holesList[F[_]: Traverse, A](fa: F[A]): List[(A, A => F[A])] = Traverse[F].toList(holes(fa))

  def builder[F[_]: Traverse, A, B](fa: F[A], children: List[B]): F[B] = {
    (Traverse[F].mapAccumL(fa, children) {
      case (x :: xs, _) => (xs, x)
      case _ => sys.error("Not enough children")
    })._2
  }

  def project[F[_], A](index: Int, fa: F[A])(implicit F: Foldable[F]): Option[A] =
   if (index < 0) None
   else F.foldMap(fa)(_ :: Nil).drop(index).headOption

  def sizeF[F[_]: Foldable, A](fa: F[A]): Int = Foldable[F].foldLeft(fa, 0)((a, _) => a + 1)
}

sealed trait attr extends term with holes {
  def attrUnit[F[_]: Functor](term: Term[F]): Cofree[F, Unit] = attrK(term, ())

  def attrK[F[_]: Functor, A](term: Term[F], k: A): Cofree[F, A] = {
    Cofree(k, Functor[F].map(term.unFix)(attrK(_, k)(Functor[F])))
  }

  def attrSelf[F[_]: Functor](term: Term[F]): Cofree[F, Term[F]] = {
    Cofree(term, Functor[F].map(term.unFix)(attrSelf(_)(Functor[F])))
  }

  def applyNT[F[_]: Functor, G[_], A](attr: Cofree[F, A])(nt: F ~> G):
      Cofree[G, A] =
    Cofree(attr.head, nt(attr.tail.map(applyNT(_)(nt))))

  def applyNTM[F[_]: Traverse, M[_]: Monad, G[_], A](attr: Cofree[F, A])(nt: F ~> ({ type lam[X] = M[G[X]] })#lam):
      M[Cofree[G, A]] =
    attr.tail.traverse(applyNTM(_)(nt)).flatMap(nt(_).map(Cofree(attr.head, _)))

  def children[F[_], A](attr: Cofree[F, A])(implicit F: Foldable[F]): List[Cofree[F, A]] =
    F.foldMap(attr.tail)(_ :: Nil)

  def universe[F[_], A](attr: Cofree[F, A])(implicit F: Foldable[F]): List[Cofree[F, A]] =
      attr :: children(attr).flatMap(universe(_))

  def forget[F[_], A](attr: Cofree[F, A])(implicit F: Functor[F]): Term[F] =
    Term(F.map(attr.tail)(forget[F, A](_)))

  implicit def CofreeRenderTree[F[_], A](implicit F: Foldable[F], RF: RenderTree[F[_]], RA: RenderTree[A]) = new RenderTree[Cofree[F, A]] {
    override def render(attr: Cofree[F, A]) = {
      val term = RF.render(attr.tail)
      val ann = RA.render(attr.head)
      NonTerminal(term.label,
        (if (ann.children.isEmpty) NonTerminal("", ann :: Nil, List("Annotation")) else ann.copy(label="", nodeType=List("Annotation"))) ::
          children(attr).map(render(_)),
        term.nodeType)
    }
  }

  // These lifts are largely useful when you want to zip a cata (or ana) with
  // some more complicated algebra.

  def liftPara[F[_]: Functor, A](f: F[A] => A): F[(Term[F], A)] => A =
    node => f(node.map(_._2))

  def liftHisto[F[_]: Functor, A](f: F[A] => A): F[Cofree[F, A]] => A =
    node => f(node.map(_.head))

  def liftApo[F[_]: Functor, A](f: A => F[A]): A => F[Term[F] \/ A] =
    f(_).map(\/-(_))

  def liftFutu[F[_]: Functor, A](f: A => F[A]): A => F[Free[F, A]] =
    f(_).map(Free.pure(_))

  // roughly DownStar(f) *** DownStar(g)
  def zipCata[F[_]: Unzip, A, B](f: F[A] => A, g: F[B] => B):
      F[(A, B)] => (A, B) =
    node => node.unfzip.bimap(f, g)

  def zipPara[F[_]: Functor, A, B](f: F[(Term[F], A)] => A, g: F[(Term[F], B)] => B):
      F[(Term[F], (A, B))] => (A, B) =
    node => (f(node.map(({ (x: (A, B)) => x._1 }).second)), g(node.map(({ (x: (A, B)) => x._2 }).second)))

  // Inherited: inherit, inherit2, inherit3, inheritM, inheritM_
  def inherit[F[_], A, B](tree: Cofree[F, A], b: B)(f: (B, Cofree[F, A]) => B)(implicit F: Functor[F]): Cofree[F, B] = {
    val b2 = f(b, tree)
    Cofree[F, B](b2, F.map(tree.tail)(inherit(_, b2)(f)(F)))
  }

  // TODO: Top down folds

  def transform[F[_], A](attrfa: Cofree[F, A])(f: A => Option[Cofree[F, A]])(implicit F: Functor[F]): Cofree[F, A] = {
    val a = attrfa.head
    f(a).map(transform(_)(f)(F))
      .getOrElse(Cofree(a, F.map(attrfa.tail)(transform(_)(f)(F))))
  }

  def swapTransform[F[_], A, B](attrfa: Cofree[F, A])(f: A => B \/ Cofree[F, B])(implicit F: Functor[F]): Cofree[F, B] = {
    lazy val fattrfb = F.map(attrfa.tail)(swapTransform(_)(f)(F))

    f(attrfa.head).fold(Cofree(_, fattrfb), identity)
  }

  def sequenceUp[F[_], G[_], A](attr: Cofree[F, G[A]])(implicit F: Traverse[F], G: Applicative[G]): G[Cofree[F, A]] = {
    val ga : G[A] = attr.head
    val fgattr : F[G[Cofree[F, A]]] = F.map(attr.tail)(t => sequenceUp(t)(F, G))

    val gfattr : G[F[Cofree[F, A]]] = F.traverseImpl(fgattr)(identity)

    G.apply2(gfattr, ga)((node, attr) => Cofree(attr, node))
  }

  def sequenceDown[F[_], G[_], A](attr: Cofree[F, G[A]])(implicit F: Traverse[F], G: Applicative[G]): G[Cofree[F, A]] = {
    val ga : G[A] = attr.head
    val fgattr : F[G[Cofree[F, A]]] = F.map(attr.tail)(t => sequenceDown(t)(F, G))

    val gfattr : G[F[Cofree[F, A]]] = F.traverseImpl(fgattr)(identity)

    G.apply2(ga, gfattr)(Cofree(_, _))
  }

  /**
   * Zips two attributed nodes together. This is unsafe in the sense that the
   * user is responsible for ensuring both left and right parameters have the
   * same shape (i.e. represent the same tree).
   */
  def unsafeZip2[F[_]: Traverse, A, B](left: Cofree[F, A], right: Cofree[F, B]): Cofree[F, (A, B)] = {
    val lattr: A = left.head
    val lunAnn: F[Cofree[F, A]] = left.tail

    val lunAnnL: List[Cofree[F, A]] = Foldable[F].toList(lunAnn)

    val rattr: B = right.head
    val runAnn: F[Cofree[F, B]] = right.tail

    val runAnnL: List[Cofree[F, B]] = Foldable[F].toList(runAnn)

    val abs: List[Cofree[F, (A, B)]] = lunAnnL.zip(runAnnL).map { case ((a, b)) => unsafeZip2(a, b) }

    val fabs : F[Cofree[F, (A, B)]] = builder(lunAnn, abs)

    Cofree((lattr, rattr), fabs)
  }
}

trait binding extends attr {
  trait Binder[F[_]] {
    type G[A]

    def initial[A]: G[A]

    // Extracts bindings from a node:
    def bindings[A](t: F[Term[F]], b: G[A])(f: F[Term[F]] => A): G[A]

    // Possibly binds a free term to its definition:
    def subst[A](t: F[Term[F]], b: G[A]): Option[A]
  }

  def boundCata[F[_]: Functor, A](t: Term[F])(f: F[A] => A)(implicit B: Binder[F]): A = {
    def loop(t: F[Term[F]], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => loop(x.unFix, newB))))
    }

    loop(t.unFix, B.initial)
  }

  def boundPara[F[_]: Functor, A](t: Term[F])(f: F[(Term[F], A)] => A)(implicit B: Binder[F]): A = {
    def loop(t: F[Term[F]], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => (x, loop(x.unFix, newB)))))
    }

    loop(t.unFix, B.initial)
  }
}

object fixplate extends binding
