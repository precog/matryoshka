package slamdata.engine

import scalaz._
import Scalaz._

package object fp {
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

  implicit def NodeRendererToShow[N: NodeRenderer] = new Show[N] {
    override def show(v: N) = {
      ShowTree.showTree(v)
    }
  }

  implicit def Tuple2NodeRenderer[A: NodeRenderer, B: NodeRenderer] = new NodeRenderer[(A, B)] {
    var RA = implicitly[NodeRenderer[A]]
    var RB = implicitly[NodeRenderer[B]]
    override def render(t: (A, B)) =
      NonTerminal("tuple", RA.render(t._1) ::
                            RB.render(t._2) ::
                            Nil)
  }

  // TODO: this implicit conflicts with the previous one, but we'd like to flatten these nested tuples out
  // when they're used in annotations. 
  // implicit def LeftTuple3NodeRenderer[A: NodeRenderer, B: NodeRenderer, C: NodeRenderer] = new NodeRenderer[((A, B), C)] {
  //   var RA = implicitly[NodeRenderer[A]]
  //   var RB = implicitly[NodeRenderer[B]]
  //   var RC = implicitly[NodeRenderer[C]]
  //   override def render(t: ((A, B), C)) =
  //     NonTerminal("tuple", RA.render(t._1._1) ::
  //                           RB.render(t._1._2) ::
  //                           RC.render(t._2) ::
  //                           Nil)
  // }
  
  implicit def OptionNodeRenderer[A: NodeRenderer] = new NodeRenderer[Option[A]] {
    var RA = implicitly[NodeRenderer[A]]
    override def render(o: Option[A]) = o match {
      case Some(a) => RA.render(a)
      case None => Terminal("None")
    }
  }
}
