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
      f(F.map(unFix)(_.cata(f)(F)))

    def para[A](f: F[(Term[F], A)] => A)(implicit F: Functor[F]): A =
      f(F.map(unFix)(t => t -> t.para(f)(F)))

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

    implicit def TermEqual[F[_]](implicit F: Equal ~> λ[α => Equal[F[α]]]):
        Equal[Term[F]] =
      Equal.equal { (a, b) => F(TermEqual[F]).equal(a.unFix, b.unFix) }
  }
  object Term extends TermInstances {
  }

  def apo[F[_], A](a: A)(f: A => F[Term[F] \/ A])(implicit F: Functor[F]): Term[F] = {
    Term(F.map(f(a)) {
      case -\/(term) => term
      case \/-(a)    => apo(a)(f)
    })
  }

  def ana[F[_], A](a: A)(f: A => F[A])(implicit F: Functor[F]): Term[F] =
    Term(F.map(f(a))(ana(_)(f)(F)))

  def hylo[F[_], A, B](a: A)(f: F[B] => B, g: A => F[A])(implicit F: Functor[F]): B =
    f(F.map(g(a))(hylo(_)(f, g)(F)))

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

sealed trait zips {
  def unzipF[F[_]: Functor, A, B](f: F[(A, B)]): (F[A], F[B]) = {
    val F = Functor[F]

    (F.map(f)(_._1), F.map(f)(_._2))
  }
}

sealed trait attr extends term with zips with holes {
  def attrUnit[F[_]: Functor](term: Term[F]): Cofree[F, Unit] = attrK(term, ())

  def attrK[F[_]: Functor, A](term: Term[F], k: A): Cofree[F, A] = {
    Cofree(k, Functor[F].map(term.unFix)(attrK(_, k)(Functor[F])))
  }

  def attrSelf[F[_]: Functor](term: Term[F]): Cofree[F, Term[F]] = {
    Cofree(term, Functor[F].map(term.unFix)(attrSelf(_)(Functor[F])))
  }

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

  def histo[F[_], A](t: Term[F])(f: F[Cofree[F, A]] => A)(implicit F: Functor[F]): A = {
    def g: Term[F] => Cofree[F, A] = { t =>
      Cofree(histo(t)(f)(F), F.map(t.unFix)(g))
    }

    f(F.map(t.unFix)(g))
  }

  def futu[F[_], A](a: A)(f: A => F[Free[F, A]])(implicit F: Functor[F]):
      Term[F] = {
    def g: Free[F, A] => Term[F] =
      _.resume.fold(fcoattr => Term(F.map(fcoattr)(g)), κ(futu(a)(f)(F)))

    Term(F.map(f(a))(g))
  }

  def synthCata[F[_]: Functor, A](term: Term[F])(f: F[A] => A): Cofree[F, A] = {

    val fattr: F[Cofree[F, A]] = Functor[F].map(term.unFix)(t => synthCata(t)(f))
    val fa: F[A] = Functor[F].map(fattr)(_.head)

    Cofree(f(fa), fattr)
  }

  def scanCata[F[_]: Functor, A, B](attr0: Cofree[F, A])(f: (A, F[B]) => B): Cofree[F, B] = {
    val fattr: F[Cofree[F, B]] = Functor[F].map(attr0.tail)(t => scanCata(t)(f))
    val b : F[B] = Functor[F].map(fattr)(_.head)

    Cofree(f(attr0.head, b), fattr)
  }

  def synthPara2[F[_]: Functor, A](term: Term[F])(f: F[(Term[F], A)] => A): Cofree[F, A] = {
    scanPara(attrUnit(term))((_, ffab) => f(Functor[F].map(ffab) { case (tf, a, b) => (tf, b) }))
  }

  def synthPara3[F[_]: Functor, A](term: Term[F])(f: (Term[F], F[A]) => A): Cofree[F, A] = {
    scanPara(attrUnit(term))((attrfa, ffab) => f(forget(attrfa), Functor[F].map(ffab)(_._3)))
  }

  def scanPara0[F[_], A, B](term: Cofree[F, A])(f: (Cofree[F, A], F[Cofree[F, (A, B)]]) => B)(implicit F: Functor[F]): Cofree[F, B] = {
    def loop(term: Cofree[F, A]): Cofree[F, (A, B)] = {
      val rec: F[Cofree[F, (A, B)]] = F.map(term.tail)(loop _)

      val a = term.head
      val b = f(term, rec)

      Cofree((a, b), rec)
    }

    loop(term).map(_._2)
  }

  def scanPara[F[_], A, B](attr: Cofree[F, A])(f: (Cofree[F, A], F[(Term[F], A, B)]) => B)(implicit F: Functor[F]): Cofree[F, B] = {
    scanPara0[F, A, B](attr) {
      case (attrfa, fattrfab) =>
        val ftermab = F.map(fattrfab) { (attrfab: Cofree[F, (A, B)]) =>
          val (a, b) = attrfab.head

          (forget(attrfab), a, b)
        }

        f(attrfa, ftermab)
    }
  }

  def scanPara2[F[_]: Functor, A, B](attr: Cofree[F, A])(f: (A, F[(Term[F], A, B)]) => B): Cofree[F, B] = {
    scanPara(attr)((attrfa, ffab) => f(attrfa.head, ffab))
  }

  def scanPara3[F[_]: Functor, A, B](attr: Cofree[F, A])(f: (Cofree[F, A], F[B]) => B): Cofree[F, B] = {
    scanPara(attr)((attrfa, ffab) => f(attrfa, Functor[F].map(ffab)(_._3)))
  }

  def synthZygo_[F[_]: Functor, A, B](term: Term[F])(f: F[B] => B, g: F[(B, A)] => A): Cofree[F, A] = {
    synthZygoWith[F, A, B, A](term)((b: B, a: A) => a, f, g)
  }

  def synthZygo[F[_]: Functor, A, B](term: Term[F])(f: F[B] => B, g: F[(B, A)] => A): Cofree[F, (B, A)] = {
    synthZygoWith[F, A, B, (B, A)](term)((b: B, a: A) => (b, a), f, g)
  }

  def synthZygoWith[F[_]: Functor, A, B, C](term: Term[F])(f: (B, A) => C, g: F[B] => B, h: F[(B, A)] => A): Cofree[F, C] = {
    def loop(term: Term[F]): ((B, A), Cofree[F, C]) = {
      val (fba, s) : (F[(B, A)], F[Cofree[F,C]]) = unzipF(Functor[F].map(term.unFix)(loop _))
      val b : B = g(Functor[F].map(fba)(_._1))
      val a : A = h(fba)
      val c : C = f(b, a)

      ((b, a), Cofree(c, s))
    }

    loop(term)._2
  }

  // synthAccumCata, synthAccumPara2, mapAccumCata, synthCataM, synthParaM, synthParaM2

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


  // Questionable value...
  def circulate[F[_], A, B](tree: Cofree[F, A])(f: A => B, up: (B, B) => B, down: (B, B) => B)(implicit F: Traverse[F]): Cofree[F, B] = {
    val pullup: Cofree[F, B] = scanPara[F, A, B](tree) { (attr: Cofree[F, A], fa: F[(Term[F], A, B)]) =>
      F.foldLeft(fa, f(attr.head))((acc, t) => up(up(f(t._2), t._3), acc))
    }

    def pushdown(attr: Cofree[F, B]): Cofree[F, B] = {
      val b1 = attr.head

      Cofree[F, B](b1, F.map(attr.tail) { attr =>
        val b2 = attr.head

        val b3 = down(b1, b2)

        pushdown(Cofree[F, B](b3, attr.tail))
      })
    }

    pushdown(pullup)
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

  def context[F[_]](term: Term[F])(implicit F: Traverse[F]): Cofree[F, Term[F] => Term[F]] = {
    def loop(f: Term[F] => Term[F]): Cofree[F, Term[F] => Term[F]] = {
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
  case class PhaseM[M[_], F[_], A, B](value: Cofree[F, A] => M[Cofree[F, B]]) extends (Cofree[F, A] => M[Cofree[F, B]]) {
    def apply(x: Cofree[F, A]) = value(x)
  }

  def liftPhase[M[_]: Monad, F[_], A, B](phase: Phase[F, A, B]): PhaseM[M, F, A, B] = {
    PhaseM(attr => Monad[M].point(phase(attr)))
  }

  /**
   * A non-monadic phase. This is only interesting for phases that cannot produce
   * errors and don't need state.
   */
  type Phase[F[_], A, B] = PhaseM[Id, F, A, B]

  def Phase[F[_], A, B](x: Cofree[F, A] => Cofree[F, B]): Phase[F, A, B] = PhaseM[Id, F, A, B](x)

  /**
   * A phase that can produce errors. An error is captured using the left side of \/.
   */
  type PhaseE[F[_], E, A, B] = PhaseM[({type f[X] = E \/ X})#f, F, A, B]

  def PhaseE[F[_], E, A, B](x: Cofree[F, A] => E \/ Cofree[F, B]): PhaseE[F, E, A, B] = {
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

  def PhaseS[F[_], S, A, B](x: Cofree[F, A] => State[S, Cofree[F, B]]): PhaseS[F, S, A, B] = {
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
    type CofreeF[X] = Cofree[F, X]

    def arr[A, B](f: A => B): Arr[A, B] = PhaseM(attr => M.point(attr.map(f)))

    def first[A, B, C](f: Arr[A, B]): Arr[(A, C), (B, C)] = PhaseM { (attr: Cofree[F, (A, C)]) =>
      val attrA = Functor[CofreeF].map(attr)(_._1)

      (f(attrA) |@| M.point(Functor[CofreeF].map(attr)(_._2)))(unsafeZip2(_, _))
    }

    def id[A]: Arr[A, A] = PhaseM(attr => M.point(attr))

    def compose[A, B, C](f: Arr[B, C], g: Arr[A, B]): Arr[A, C] =
      PhaseM { (attr: Cofree[F, A]) => g(attr).flatMap(f) }
  }

  implicit class ToPhaseMOps[M[_]: Monad, F[_]: Traverse, A, B](self: PhaseM[M, F, A, B]) {
    def >>> [C](that: PhaseM[M, F, B, C]) = PhaseMArrow[M, F].compose(that, self)

    def &&& [C](that: PhaseM[M, F, A, C]) = PhaseMArrow[M, F].combine(self, that)
    def *** [C, D](that: PhaseM[M, F, C, D]) = PhaseMArrow[M, F].split(self, that)

    def first[C]: PhaseM[M, F, (A, C), (B, C)] = PhaseMArrow[M, F].first(self)

    def second[C]: PhaseM[M, F, (C, A), (C, B)] = PhaseM { (attr: Cofree[F, (C, A)]) =>
      first.map((t: (B, C)) => (t._2, t._1))(attr.map((t: (C, A)) => (t._2, t._1)))
    }

    def map[C](f: B => C): PhaseM[M, F, A, C] = PhaseM((attr: Cofree[F, A]) => Functor[M].map(self(attr))(_.map(f)))

    def dup: PhaseM[M, F, A, (B, B)] = map(v => (v, v))

    def fork[C, D](left: PhaseM[M, F, B, C], right: PhaseM[M, F, B, D]): PhaseM[M, F, A, (C, D)] = PhaseM { (attr: Cofree[F, A]) =>
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
    type CofreeF[A] = Cofree[F, A]

    // The combination of an attributed node and the bindings valid in this scope.
    type `CofreeF * G`[A] = (CofreeF[A], G[A])

    // A function that can lift an attribute into an attributed node.
    type Unsubst[A] = A => CofreeF[A]

    type Subst[A] = Option[(CofreeF[A], Forall[Unsubst])]

    // Extracts bindings from a node:
    val bindings: CofreeF ~> G

    // Possibly binds a free term to its definition:
    val subst: `CofreeF * G` ~> Subst

    def apply[M[_], A, B](phase: PhaseM[M, F, A, B])(implicit F: Traverse[F], G: Monoid[G[A]], M: Functor[M]): PhaseM[M, F, A, B] = PhaseM[M, F, A, B] { attrfa =>
      def subst0(ga0: G[A], attrfa: CofreeF[A]): CofreeF[(A, Option[Forall[Unsubst]])] = {
        // Possibly swap out this node for another node:
        val optT: Option[(CofreeF[A], Forall[Unsubst])] = subst((attrfa, ga0))

        val (attrfa2, optF) = optT.map(tuple => tuple._1 -> Some(tuple._2)).getOrElse(attrfa -> None)

        // Add any new bindings:
        val ga: G[A] = G.append(ga0, bindings(attrfa2))

        // Recursively apply binding:
        Cofree[F, (A, Option[Forall[Unsubst]])](attrfa2.head -> optF, F.map(attrfa2.tail)(subst0(ga, _)))
      }

      val attrft: CofreeF[(A, Option[Forall[Unsubst]])] = subst0(G.zero, attrfa)

      val mattrfb: M[CofreeF[B]] = phase(attrft.map(_._1))

      M.map(mattrfb) { attrfb =>
        val zipped = unsafeZip2(attrfb, attrft.map(_._2))

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
