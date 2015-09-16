/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.RenderTree.ops._

import scalaz._; import Liskov._; import Scalaz._
import scalaz.concurrent.Task
import scalaz.effect._
import simulacrum.{typeclass, op}

sealed trait LowerPriorityTreeInstances {
  implicit def Tuple2RenderTree[A, B](implicit RA: RenderTree[A], RB: RenderTree[B]) =
    new RenderTree[(A, B)] {
      def render(t: (A, B)) =
        NonTerminal("tuple" :: Nil, None,
          RA.render(t._1) ::
            RB.render(t._2) ::
            Nil)
    }
}

sealed trait LowPriorityTreeInstances extends LowerPriorityTreeInstances {
  implicit def LeftTuple3RenderTree[A, B, C](implicit RA: RenderTree[A], RB: RenderTree[B], RC: RenderTree[C]) =
    new RenderTree[((A, B), C)] {
      def render(t: ((A, B), C)) =
        NonTerminal("tuple" :: Nil, None,
          RA.render(t._1._1) ::
            RB.render(t._1._2) ::
            RC.render(t._2) ::
            Nil)
    }
}

sealed trait TreeInstances extends LowPriorityTreeInstances {
  implicit def LeftTuple4RenderTree[A, B, C, D](implicit RA: RenderTree[A], RB: RenderTree[B], RC: RenderTree[C], RD: RenderTree[D]) =
    new RenderTree[(((A, B), C), D)] {
      def render(t: (((A, B), C), D)) =
        NonTerminal("tuple" :: Nil, None,
           RA.render(t._1._1._1) ::
            RB.render(t._1._1._2) ::
            RC.render(t._1._2) ::
            RD.render(t._2) ::
            Nil)
    }

  implicit def EitherRenderTree[A, B](implicit RA: RenderTree[A], RB: RenderTree[B]) =
    new RenderTree[A \/ B] {
      def render(v: A \/ B) =
        v match {
          case -\/ (a) => NonTerminal("-\\/" :: Nil, None, RA.render(a) :: Nil)
          case \/- (b) => NonTerminal("\\/-" :: Nil, None, RB.render(b) :: Nil)
        }
    }

  implicit def OptionRenderTree[A](implicit RA: RenderTree[A]) =
    new RenderTree[Option[A]] {
      def render(o: Option[A]) = o match {
        case Some(a) => RA.render(a)
        case None => Terminal("None" :: "Option" :: Nil, None)
      }
    }

  implicit def ListRenderTree[A](implicit RA: RenderTree[A]) =
    new RenderTree[List[A]] {
      def render(v: List[A]) = NonTerminal(List("List"), None, v.map(RA.render))
    }

  implicit def ListMapRenderTree[K, V](implicit RV: RenderTree[V]) =
    new RenderTree[ListMap[K, V]] {
      def render(v: ListMap[K, V]) =
        NonTerminal("Map" :: Nil, None,
          v.toList.map { case (k, v) =>
            NonTerminal("Key" :: "Map" :: Nil, Some(k.toString), RV.render(v) :: Nil)
          })
    }

  implicit val BooleanRenderTree = RenderTree.fromToString[Boolean]("Boolean")
  implicit val IntRenderTree = RenderTree.fromToString[Int]("Int")
  implicit val DoubleRenderTree = RenderTree.fromToString[Double]("Double")
  implicit val StringRenderTree = RenderTree.fromToString[String]("String")

  // NB: RenderTree should `extend Show[A]`, but Scalaz type classes don’t mesh
  //     with Simulacrum ones.
  implicit def RenderTreeToShow[N: RenderTree] = new Show[N] {
    override def show(v: N) = v.render.show
  }
}

sealed trait ListMapInstances {
  implicit def seqW[A](xs: Seq[A]) = new SeqW(xs)
  class SeqW[A](xs: Seq[A]) {
    def toListMap[B, C](implicit ev: A <~< (B, C)): ListMap[B, C] = {
      ListMap(co[Seq, A, (B, C)](ev)(xs) : _*)
    }
  }

  implicit def TraverseListMap[K] = new Traverse[ListMap[K, ?]] with IsEmpty[ListMap[K, ?]] {
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

trait ToCatchableOps {
  trait CatchableOps[F[_], A] extends scalaz.syntax.Ops[F[A]] {
    import SKI._

    /**
      A new task which runs a cleanup task only in the case of failure, and
      ignores any result from the cleanup task.
      */
    final def onFailure(cleanup: F[_])(implicit FM: Monad[F], FC: Catchable[F]):
        F[A] =
      self.attempt.flatMap(_.fold(
        err => cleanup.attempt.flatMap(κ(FC.fail(err))),
        _.point[F]))

    /**
      A new task that ignores the result of this task, and runs another task no
      matter what.
      */
    final def ignoreAndThen[B](t: F[B])(implicit FB: Bind[F], FC: Catchable[F]):
        F[B] =
      self.attempt.flatMap(κ(t))
  }

  implicit def ToCatchableOpsFromCatchable[F[_], A](a: F[A]):
      CatchableOps[F, A] =
    new CatchableOps[F, A] { val self = a }
}

trait PartialFunctionOps {
  implicit class PFOps[A, B](self: PartialFunction[A, B]) {
    def |?| [C](that: PartialFunction[A, C]): PartialFunction[A, B \/ C] =
      Function.unlift(v =>
        self.lift(v).fold[Option[B \/ C]](
          that.lift(v).map(\/-(_)))(
          x => Some(-\/(x))))
  }
}

trait JsonOps {
  import argonaut._
  import SKI._

  def optional[A: DecodeJson](cur: ACursor): DecodeResult[Option[A]] =
    cur.either.fold(
      κ(DecodeResult(\/- (None))),
      v => v.as[A].map(Some(_)))

  def orElse[A: DecodeJson](cur: ACursor, default: => A): DecodeResult[A] =
    cur.either.fold(
      κ(DecodeResult(\/- (default))),
      v => v.as[A]
    )

  def decodeJson[A](text: String)(implicit DA: DecodeJson[A]): String \/ A = for {
    json <- Parse.parse(text)
    a <- DA.decode(json.hcursor).result.leftMap { case (exp, hist) => "expected: " + exp + "; " + hist }
  } yield a


  /* Nicely formatted, order-preserving, single-line. */
  val minspace = PrettyParams(
    "",       // indent
    "", " ",  // lbrace
    " ", "",  // rbrace
    "", " ",  // lbracket
    " ", "",  // rbracket
    "",       // lrbracketsEmpty
    "", " ",  // arrayComma
    "", " ",  // objectComma
    "", " ",  // colon
    true,     // preserveOrder
    false     // dropNullKeys
  )

  /** Nicely formatted, order-preserving, 2-space indented. */
  val multiline = PrettyParams(
    "  ",     // indent
    "", "\n",  // lbrace
    "\n", "",  // rbrace
    "", "\n",  // lbracket
    "\n", "",  // rbracket
    "",       // lrbracketsEmpty
    "", "\n",  // arrayComma
    "", "\n",  // objectComma
    "", " ",  // colon
    true,     // preserveOrder
    false     // dropNullKeys
  )
}

trait ProcessOps {
  import scalaz.stream.{Process, Cause}

  class PrOps[F[_], O](self: Process[F, O]) {
    def cleanUpWith(t: F[Unit]): Process[F, O] =
      self.onComplete(Process.eval(t).drain)

    // NB: backported from scalaz-stream master
    import Process._
    import Cause._
    final def unconsOption[F2[x] >: F[x], O2 >: O](implicit F: Monad[F2], C: Catchable[F2]): F2[Option[(O2, Process[F2, O2])]] = {
      def evaluate[F2[x] >: F[x], O2 >: O, A](await: Await[F2, A, O2])(implicit F: Monad[F2], C: Catchable[F2]): F2[Process[F2,O2]] =
        C.attempt(await.req).map { e =>
          await.rcv(EarlyCause.fromTaskResult(e)).run
        }

      self.step match {
        case Step(head, next) => head match {
          case Emit(as) => as.headOption.map(x => F.point[Option[(O2, Process[F2, O2])]](Some((x, Process.emitAll[O2](as drop 1) +: next)))) getOrElse
              new PrOps(next.continue).unconsOption
          case await: Await[F2, _, O2] => evaluate(await).flatMap(p => new PrOps(p +: next).unconsOption(F,C))
        }
        case Halt(cause) => cause match {
          case End | Kill => F.point(None)
          case _ : EarlyCause => C.fail(cause.asThrowable)
        }
      }
    }
  }

  implicit class PrOpsTask[O](self: Process[Task, O])
      extends PrOps[Task, O](self)
}

trait SKI {
  // NB: Unicode has double-struck and bold versions of the letters, which might
  //     be more appropriate, but the code points are larger than 2 bytes, so
  //     Scala doesn't handle them.

  /** Probably not useful; implemented here mostly because it's amusing. */
  def σ[A, B, C](x: A => B => C, y: A => B, z: A): C = x(z)(y(z))

  /**
   A shorter name for the constant function of 1, 2, 3, or 6 args.
   NB: the argument is eager here, so use `_ => ...` instead if you need it to be thunked.
   */
  def κ[A, B](x: B): A => B                                 = _ => x
  def κ[A, B, C](x: C): (A, B) => C                         = (_, _) => x
  def κ[A, B, C, D](x: D): (A, B, C) => D                   = (_, _, _) => x
  def κ[A, B, C, D, E, F, G](x: G): (A, B, C, D, E, F) => G = (_, _, _, _, _, _) => x

  /** A shorter name for the identity function. */
  def ι[A]: A => A = x => x
}
object SKI extends SKI

package object fp extends TreeInstances with ListMapInstances with ToCatchableOps with PartialFunctionOps with JsonOps with ProcessOps with SKI {
  sealed trait Polymorphic[F[_], TC[_]] {
    def apply[A: TC]: TC[F[A]]
  }

  @typeclass trait ShowF[F[_]] {
    def show[A](fa: F[A])(implicit sa: Show[A]): Cord
  }

  implicit def ShowShowF[F[_], A: Show, FF[A] <: F[A]](implicit FS: ShowF[F]):
      Show[FF[A]] =
    new Show[FF[A]] { override def show(fa: FF[A]) = FS.show(fa) }

  implicit def ShowFNT[F[_]](implicit SF: ShowF[F]):
      Show ~> λ[α => Show[F[α]]] =
    new (Show ~> λ[α => Show[F[α]]]) {
      def apply[α](st: Show[α]): Show[F[α]] = ShowShowF(st, SF)
    }

  @typeclass trait EqualF[F[_]] {
    @op("≟", true) def equal[A](fa1: F[A], fa2: F[A])(implicit eq: Equal[A]):
        Boolean
    @op("≠") def notEqual[A](fa1: F[A], fa2: F[A])(implicit eq: Equal[A]) =
      !equal(fa1, fa2)
  }

  implicit def EqualEqualF[F[_], A: Equal, FF[A] <: F[A]](implicit FE: EqualF[F]):
      Equal[FF[A]] =
    new Equal[FF[A]] { def equal(fa1: FF[A], fa2: FF[A]) = FE.equal(fa1, fa2) }

  implicit def EqualFNT[F[_]](implicit EF: EqualF[F]):
      Equal ~> λ[α => Equal[F[α]]] =
    new (Equal ~> λ[α => Equal[F[α]]]) {
      def apply[α](eq: Equal[α]): Equal[F[α]] = EqualEqualF(eq, EF)
    }

  @typeclass trait SemigroupF[F[_]] {
    @op("⊹", true) def append[A: Semigroup](fa1: F[A], fa2: F[A]): F[A]
  }

  def unzipDisj[A, B](ds: List[A \/ B]): (List[A], List[B]) = {
    val (as, bs) = ds.foldLeft((List[A](), List[B]())) {
      case ((as, bs), -\/ (a)) => (a :: as, bs)
      case ((as, bs),  \/-(b)) => (as, b :: bs)
    }
    (as.reverse, bs.reverse)
  }

  def parseInt(str: String): Option[Int] =
    \/.fromTryCatchNonFatal(str.toInt).toOption

  def parseBigInt(str: String): Option[BigInt] =
    \/.fromTryCatchNonFatal(BigInt(str)).toOption

  def parseDouble(str: String): Option[Double] =
    \/.fromTryCatchNonFatal(str.toDouble).toOption

  def parseBigDecimal(str: String): Option[BigDecimal] =
    \/.fromTryCatchNonFatal(BigDecimal(str)).toOption

  /** Accept a value (forcing the argument expression to be evaluated for its
    * effects), and then discard it, returning Unit. Makes it explicit that
    * you're discarding the result, and effectively suppresses the
    * "NonUnitStatement" warning from wartremover.
    */
  def ignore[A](a: A): Unit = ()

  val fromIO = new (IO ~> Task) {
    def apply[A](io: IO[A]): Task[A] = Task.delay(io.unsafePerformIO())
  }

  /** Wrapper around [[IORef]] to operate in [[Task]] */
  final class TaskRef[A](val ioRef: IORef[A]) {
    def read: Task[A] = fromIO(ioRef.read)
    def write(a: => A): Task[Unit] = fromIO(ioRef.write(a))
    def mod(f: A => A): Task[A] = fromIO(ioRef.mod(f))
  }

  object TaskRef {
    def apply[A](a: => A): Task[TaskRef[A]] =
      fromIO(IO.newIORef(a).map(ior => new TaskRef(ior)))
  }
}
