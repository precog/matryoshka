package slamdata.engine

import scalaz._
import Scalaz._

sealed trait LowPriorityTreeInstances {
  implicit def Tuple2RenderTree[A, B](implicit RA: RenderTree[A], RB: RenderTree[B]) =
    new RenderTree[(A, B)] {
      override def render(t: (A, B)) =
        NonTerminal("tuple", RA.render(t._1) ::
                              RB.render(t._2) ::
                              Nil)
    }
}

sealed trait TreeInstances extends LowPriorityTreeInstances {
  implicit def LeftTuple3RenderTree[A, B, C](implicit RA: RenderTree[A], RB: RenderTree[B], RC: RenderTree[C]) =
    new RenderTree[((A, B), C)] {
      override def render(t: ((A, B), C)) =
        NonTerminal("tuple", RA.render(t._1._1) ::
                              RB.render(t._1._2) ::
                              RC.render(t._2) ::
                              Nil)
    }

  implicit def EitherRenderTree[A, B](implicit RA: RenderTree[A], RB: RenderTree[B]) =
    new RenderTree[A \/ B] {
      override def render(v: A \/ B) =
        v match {
          case -\/ (a) => NonTerminal("-\\/", RA.render(a) :: Nil)
          case \/- (b) => NonTerminal("\\/-", RB.render(b) :: Nil)
        }
    }

  implicit def OptionRenderTree[A](implicit RA: RenderTree[A]) =
    new RenderTree[Option[A]] {
      override def render(o: Option[A]) = o match {
        case Some(a) => RA.render(a)
        case None => Terminal("None")
      }
    }

  implicit def RenderTreeToShow[N: RenderTree] = new Show[N] {
    override def show(v: N) = RenderTree.show(v)
  }
}

package object fp extends TreeInstances {
  sealed trait Polymorphic[F[_], TC[_]] {
    def apply[A: TC]: TC[F[A]]
  }

  trait ShowF[F[_]] {
    def show[A](fa: F[A]): Cord
  }
  trait EqualF[F[_]] {
    def equal[A](fa1: F[A], fa2: F[A])(implicit eq: Equal[A]): Boolean
  }
  trait MonoidF[F[_]] {
    def append[A: Monoid](fa1: F[A], fa2: F[A]): F[A]
  }

  trait Empty[F[_]] {
    def empty[A]: F[A]
  }
  object Empty {
    def apply[F[+_]](value: F[Nothing]): Empty[F] = new Empty[F] {
      def empty[A]: F[A] = value
    }
  }

  implicit val SymbolEqual: Equal[Symbol] = new Equal[Symbol] {
    def equal(v1: Symbol, v2: Symbol): Boolean = v1 == v2
  }

  implicit val SymbolOrder: Order[Symbol] = new Order[Symbol] {
    def order(x: Symbol, y: Symbol): Ordering = Order[String].order(x.name, y.name)
  }

  implicit class ListOps[A](c: List[A]) {
    def decon = c.headOption map ((_, c.tail))

    def tailOption = c.headOption map (_ => c.tail)
  }

  trait ConstrainedMonad[F[_], TC[_]] {
    def point[A: TC](a: A): F[A]

    def bind[A, B: TC](fa: F[A])(f: A => F[B]): F[B]
  }

  object cogroup {
    import \&/._

    sealed trait Instr[A] {
      def emit: List[A]
    }
    case class ConsumeLeft [A](emit: List[A]) extends Instr[A]
    case class ConsumeRight[A](emit: List[A]) extends Instr[A]
    case class ConsumeBoth [A](emit: List[A]) extends Instr[A]

    def apply[F[_], A, B, C](left: List[A], right: List[B])(f: A \&/ B => F[Instr[C]])(implicit F: Monad[F]): F[List[C]] = {
      def loop(acc: List[C], left: List[A], right: List[B]): F[List[C]] = {
        (left, right) match {
          case (lh :: lt, rh :: rt) => 
            for {
              instr <-  f(Both(lh, rh))

              emitr = instr.emit.reverse

              rec   <-  instr match {
                          case ConsumeLeft (_) => loop(emitr ::: acc, lt,   right)
                          case ConsumeRight(_) => loop(emitr ::: acc, left, rt)
                          case ConsumeBoth (_) => loop(emitr ::: acc, lt,   rt)
                        }
            } yield rec

          case (Nil, rh :: rt) => 
            for {
              instr <- f(That(rh))
              rec   <- loop(instr.emit.reverse ::: acc, Nil, rt)
            } yield rec

          case (lh :: lt, Nil) => 
            for {
              instr <- f(This(lh))
              rec   <- loop(instr.emit.reverse ::: acc, lt, Nil)
            } yield rec

          case (Nil, Nil) => F.point(acc)
        }
      }

      loop(Nil, left, right).map(_.reverse)
    }

    def stateful[S, A, B, C](left: List[A], right: List[B])(s0: S)(f: (S, A \&/ B) => (S, Instr[C])): (S, List[C]) = {
      type StateST[X] = StateT[Free.Trampoline, S, X]

      val state = apply[StateST, A, B, C](left, right) { these =>
        StateT(s => Monad[Free.Trampoline].point(f(s, these)))
      }

      state.run(s0).run
    }

    def statefulE[S, E, A, B, C](left: List[A], right: List[B])(s0: S)(f: (S, A \&/ B) => E \/ (S, Instr[C])): E \/ (S, List[C]) = {
      type EitherET[X] = EitherT[Free.Trampoline, E, X]

      type StateST[X] = StateT[EitherET, S, X]

      val state = apply[StateST, A, B, C](left, right) { these =>
        StateT[EitherET, S, Instr[C]] { s =>
          EitherT(f(s, these).point[Free.Trampoline])
        }
      }

      state.run(s0).run.run
    }
  }
}
