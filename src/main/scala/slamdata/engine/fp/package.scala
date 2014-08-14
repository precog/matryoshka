package slamdata.engine

import collection.immutable.ListMap

import scalaz._
import Scalaz._
import Liskov._
import scalaz.concurrent.{Task}

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
        case None => Terminal("", "Option" :: "None" :: Nil)
      }
    }

  implicit def RenderTreeToShow[N: RenderTree] = new Show[N] {
    override def show(v: N) = RenderTree.show(v)
  }
}

sealed trait ListMapInstances {
  implicit def seqW[A](xs: Seq[A]) = new SeqW(xs)
  class SeqW[A](xs: Seq[A]) {
    def toListMap[B, C](implicit ev: A <~< (B, C)): ListMap[B, C] = {
      ListMap(co[Seq, A, (B, C)](ev)(xs) : _*)
    }
  }

  implicit def TraverseListMap[K] = new Traverse[({type F[V] = ListMap[K,V]})#F] with IsEmpty[({type F[V] = ListMap[K,V]})#F] {
    def empty[V] = ListMap.empty[K, V]
    def plus[V](a: ListMap[K, V], b: => ListMap[K, V]) = a ++ b
    def isEmpty[V](fa: ListMap[K, V]) = fa.isEmpty
    override def map[A, B](fa: ListMap[K, A])(f: A => B) = fa.map{case (k, v) => (k, f(v))}
    def traverseImpl[G[_],A,B](m: ListMap[K,A])(f: A => G[B])(implicit G: Applicative[G]): G[ListMap[K,B]] = {
      import G.functorSyntax._
      scalaz.std.list.listInstance.traverseImpl(m.toList)({ case (k, v) => f(v) map (k -> _) }) map (_.toListMap)
    }
  }
}

trait TaskOps[A] extends scalaz.syntax.Ops[Task[A]] {
  /**
   A new task which runs a cleanup task only in the case of failure, and ignores any result
   from the cleanup task.
   */
  final def onFailure(cleanup: Task[_]): Task[A] =
    self.attempt.flatMap(_.fold(
      err => cleanup.attempt.flatMap(_ => Task.fail(err)),
      Task.now
    ))

  /**
   A new task that ignores the result of this task, and runs another task no matter what.
  */
  final def ignoreAndThen[B](t: Task[B]): Task[B] =
    self.attempt.flatMap(_ => t)
}
    
trait ToTaskOps {
  implicit def ToTaskOpsFromTask[A](a: Task[A]): TaskOps[A] = new TaskOps[A] {
    val self = a
  }
}

package object fp extends TreeInstances with ListMapInstances with ToTaskOps {
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

      def flip: Instr[A] = this match {
        case ConsumeLeft (e) => ConsumeRight(e)
        case ConsumeRight(e) => ConsumeLeft(e)
        case ConsumeBoth (e) => ConsumeBoth(e)
      }
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

  def spansM[F[_], A, B, C](l: List[A])(predicate: PartialFunction[A, B])(left: NonEmptyList[B] => F[List[C]], right: NonEmptyList[A] => F[List[C]])(implicit F: Monad[F]): F[List[C]] = {
    def collect[A, B](l: List[A])(f: NonEmptyList[A] => F[List[B]]): F[List[B]] = l.reverse match {
      case l :: ls => f(NonEmptyList.nel(l, ls)).map(_.reverse)

      case Nil => (List.empty[B]).point[F]
    }

    def spans0(acc: List[C], stage: List[B] \/ List[A], l: List[A]): F[List[C]] = l match {
      case Nil => stage.fold(collect(_)(left), collect(_)(right)).map(_ ::: acc)

      case x :: xs => predicate.lift(x) match {
        case Some(b) => 
          stage match {
            case -\/  (bs) => spans0(acc, -\/ (b :: bs), xs)

            case \/- (as) => 
              for {
                right <- collect(as)(right)
                rec   <- spans0(right ::: acc, -\/ (b :: Nil), xs)
              } yield rec
          }

        case None => 
          stage match {
            case -\/ (bs) => 
              for {
                left <- collect(bs)(left)
                rec  <- spans0(left ::: acc, \/- (x :: Nil), xs)
              } yield rec

            case \/- (as) => spans0(acc, \/- (x :: as), xs)
          }
      }
    }

    spans0(Nil, -\/ (Nil), l).map(_.reverse)
  }

  def spansOpt[A, B, C](l: List[A])(p: PartialFunction[A, B])(left: NonEmptyList[B] => Option[List[C]], right: NonEmptyList[A] => Option[List[C]]): Option[List[C]] = {
    type OptionFree[X] = OptionT[Free.Trampoline, X]

    def lift[A](o: Option[A]): OptionFree[A] = OptionT(o.point[Free.Trampoline])

    spansM(l)(p)(l => lift(left(l)), r => lift(right(r))).run.run
  }

  def spansId[A, B, C](l: List[A])(p: PartialFunction[A, B])(left: NonEmptyList[B] => List[C], right: NonEmptyList[A] => List[C]): List[C] = {
    type M[X] = Free.Trampoline[X]

    spansM(l)(p)(l => left(l).point[M], r => right(r).point[M]).run
  }

  def collectWhile[A, B](xs: List[A])(f: PartialFunction[A, B]): (List[B], List[A]) = {
    def loop(left: List[B], right: List[A]): (List[B], List[A]) = right match {
      case x :: right if f.isDefinedAt(x) => loop(f(x) :: left, right)
      case _ => (left.reverse, right)
    }

    loop(Nil, xs)
  }
  
  def unzipDisj[A, B](ds: List[A \/ B]): (List[A], List[B]) =
    ds.foldLeft((List[A](), List[B]())) {
      case ((as, bs), -\/ (a)) => (a :: as, bs)
      case ((as, bs),  \/-(b)) => (as, b :: bs)
    }
}
