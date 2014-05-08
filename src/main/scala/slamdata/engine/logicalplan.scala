package slamdata.engine

import scalaz._

import scalaz.std.string._
import scalaz.std.list._
import scalaz.std.map._

import slamdata.engine.fp._

sealed trait LogicalPlan[+A] {
  import LogicalPlan._

  def fold[Z](
      read:       String  => Z, 
      constant:   Data    => Z,
      join:       (A, A, JoinType, JoinRel, A, A) => Z,
      invoke:     (Func, List[A]) => Z,
      free:       Symbol  => Z,
      let:        (Map[Symbol, A], A) => Z
    ): Z = this match {
    case Read(x)              => read(x)
    case Constant(x)          => constant(x)
    case Join(left, right, 
              tpe, rel, 
              lproj, rproj)   => join(left, right, tpe, rel, lproj, rproj)
    case Invoke(func, values) => invoke(func, values)
    case Free(name)           => free(name)
    case Let(bind, in)        => let(bind, in)
  }
}

object LogicalPlan {
  implicit val LogicalPlanFunctor = new Functor[LogicalPlan] with Foldable[LogicalPlan] with Traverse[LogicalPlan] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan[B]] = {
      fa match {
        case x @ Read(_) => G.point(x)
        case x @ Constant(_) => G.point(x)
        case Join(left, right, tpe, rel, lproj, rproj) => 
          G.apply4(f(left), f(right), f(lproj), f(rproj))(Join(_, _, tpe, rel, _, _))
        case Invoke(func, values) => G.map(Traverse[List].sequence(values.map(f)))(Invoke(func, _))
        case x @ Free(_) => G.point(x)
        case Let(let0, in0) => {
          type MapSymbol[X] = Map[Symbol, X]

          val let: G[Map[Symbol, B]] = Traverse[MapSymbol].sequence(let0.mapValues(f))
          val in: G[B] = f(in0)

          G.apply2(let, in)(Let(_, _))
        }
      }
    }

    override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] = {
      v match {
        case x @ Read(_) => x
        case x @ Constant(_) => x
        case Join(left, right, tpe, rel, lproj, rproj) =>
          Join(f(left), f(right), tpe, rel, f(lproj), f(rproj))
        case Invoke(func, values) => Invoke(func, values.map(f))
        case x @ Free(_) => x
        case Let(let, in) => Let(let.mapValues(f), f(in))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case x @ Read(_) => F.zero
        case x @ Constant(_) => F.zero
        case Join(left, right, tpe, rel, lproj, rproj) =>
          F.append(F.append(f(left), f(right)), F.append(f(lproj), f(rproj)))
        case Invoke(func, values) => Foldable[List].foldMap(values)(f)
        case x @ Free(_) => F.zero
        case Let(let, in) => {
          type MapSymbol[X] = Map[Symbol, X]

          F.append(Foldable[MapSymbol].foldMap(let)(f), f(in))
        }
      }
    }

    override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case x @ Read(_) => z
        case x @ Constant(_) => z
        case Join(left, right, tpe, rel, lproj, rproj) =>
          f(left, f(right, f(lproj, f(rproj, z))))
        case Invoke(func, values) => Foldable[List].foldRight(values, z)(f)
        case x @ Free(_) => z
        case Let(let, in) => {
          type MapSymbol[X] = Map[Symbol, X]

          Foldable[MapSymbol].foldRight(let, f(in, z))(f)
        }
      }
    }
  }
  implicit val ShowLogicalPlan: Show[LogicalPlan[_]] = new Show[LogicalPlan[_]] {
    override def show(v: LogicalPlan[_]): Cord = v match {
      case Read(name) => Cord("Read(" + name + ")")
      case Constant(data) => Cord(data.toString)
      case Join(left, right, tpe, rel, lproj, rproj) => Cord("Join(" + tpe + ")")
      case Invoke(func, values) => Cord("Invoke(" + func.name + ")")
      case Free(name) => Cord(name.toString)
      case Let(let, in) => Cord("Let(" + let.keys.mkString(", ") + ")")
    }
  }
  implicit val EqualFLogicalPlan = new fp.EqualF[LogicalPlan] {
    def equal[A](v1: LogicalPlan[A], v2: LogicalPlan[A])(implicit A: Equal[A]): Boolean = (v1, v2) match {
      case (Read(n1), Read(n2)) => n1 == n2
      case (Constant(d1), Constant(d2)) => d1 == d2
      case (Join(l1, r1, tpe1, rel1, lproj1, rproj1), 
            Join(l2, r2, tpe2, rel2, lproj2, rproj2)) => 
        A.equal(l1, l2) && A.equal(r1, r2) && A.equal(lproj1, lproj2) && A.equal(rproj1, rproj2) && tpe1 == tpe2
      case (Invoke(f1, v1), Invoke(f2, v2)) => Equal[List[A]].equal(v1, v2) && f1 == f2
      case (Free(n1), Free(n2)) => n1 == n2
      case (Let(l1, i1), Let(l2, i2)) => A.equal(i1, i2) && Equal[Map[Symbol, A]].equal(l1, l2)
      case _ => false
    }
  }

  case class Read(resource: String) extends LogicalPlan[Nothing]

  case class Constant(data: Data) extends LogicalPlan[Nothing]

  case class Join[A](left: A, right: A, 
                     joinType: JoinType, joinRel: JoinRel, 
                     leftProj: A, rightProj: A) extends LogicalPlan[A]

  case class Invoke[A](func: Func, values: List[A]) extends LogicalPlan[A]

  case class Free(name: Symbol) extends LogicalPlan[Nothing]

  case class Let[A](let: Map[Symbol, A], in: A) extends LogicalPlan[A]

  import slamdata.engine.analysis._
  import fixplate._

  type LPTerm = Term[LogicalPlan]

  type LP = LogicalPlan[LPTerm]

  type LPAttr[A] = Attr[LogicalPlan, A]

  type LPPhase[A, B] = Phase[LogicalPlan, A, B]

  def read(resource: String): LPTerm = Term[LogicalPlan](Read(resource))
  def constant(data: Data): LPTerm = Term[LogicalPlan](Constant(data))
  def join(left: LPTerm, right: LPTerm, joinType: JoinType, joinRel: JoinRel, leftProj: LPTerm, rightProj: LPTerm): LPTerm = 
    Term(Join(left, right, joinType, joinRel, leftProj, rightProj))
  def invoke(func: Func, values: List[LPTerm]): LPTerm = Term(Invoke(func, values))
  def free(symbol: Symbol): Term[LogicalPlan] = Term[LogicalPlan](Free(symbol))
  def let(let: Map[Symbol, Term[LogicalPlan]], in: Term[LogicalPlan]): Term[LogicalPlan] = Term[LogicalPlan](Let(let, in))

  /**
   * A phase that can be used to propagate annotations through let bindings.
   */
  def boundToFree[M[_], A](implicit A: Semigroup[A], M: Monad[M]): PhaseM[M, LogicalPlan, A, A] = PhaseM[M, LogicalPlan, A, A] { attr =>
    val empty = Map.empty[Symbol, A]

    val inherited = inherit(attr, empty) { (map: Map[Symbol, A], attr: Attr[LogicalPlan, A]) =>
      attr.unFix.unAnn.fold[Map[Symbol, A]](
        read      = _ => map,
        constant  = _ => map,
        join      = (_, _, _, _, _, _) => map,
        invoke    = (_, _) => map,
        free      = _ => map,
        let       = (let, attr) => map ++ let.mapValues(_.unFix.attr)
      )
    }

    M.point(attrMap2(unsafeZip2(attr, inherited)) { (attr: Attr[LogicalPlan, (A, Map[Symbol, A])]) =>
      val (a, map) = attr.unFix.attr

      attr.unFix.unAnn.fold[A](
        read      = _ => a,
        constant  = _ => a,
        join      = (_, _, _, _, _, _) => a,
        invoke    = (_, _) => a,
        free      = symbol => map.get(symbol).map(A.append(a, _)).getOrElse(a),
        let       = (let, attr) => a
      )
    })
  }

  // Defining the function like this let's us use type synonyms to avoid having to create monstrous type lambdas.
  trait Bound[F[_], G[_]] {
    type AttrF[X] = Attr[F, X]

    type `AttrF * G`[X] = (AttrF[X], G[X])

    type Subst[X] = Option[(AttrF[X], Empty[AttrF])]

    def apply[A, B](attrfa: AttrF[A])(bound: AttrF ~> G, subst: `AttrF * G` ~> Subst)(f: AttrF[A] => AttrF[B])
        (implicit F: Traverse[F], G: Monoid[G[A]]): AttrF[B] = {
      
      def subst0(acc: G[A], attrfa: AttrF[A]): AttrF[(A, Option[Empty[AttrF]])] = {
        // Add new bindings to the old bindings:
        val ga: G[A] = G.append(acc, bound(attrfa))

        // With the current bindings, substitute on the current node:
        val optT: Option[(AttrF[A], Empty[AttrF])] = subst((attrfa, ga))

        // Take the current substitution or the original node if no substitution was performed:
        val (attrfa2, optEmpty) = optT.map(tuple => tuple._1 -> Some(tuple._2)).getOrElse(attrfa -> None)

        // Perform recursive substitution:
        Attr[F, (A, Option[Empty[AttrF]])](attrfa2.unFix.attr -> optEmpty, F.map(attrfa2.unFix.unAnn)(subst0(ga, _)))
      }

      val attrft: AttrF[(A, Option[Empty[AttrF]])] = subst0(G.zero, attrfa)

      val attrfb: AttrF[B] = f(attrMap(attrft)(_._1))
      val attrfempty: AttrF[Option[Empty[AttrF]]] = attrMap(attrft)(_._2)

      val attrfbs = AttrTraverse[F].toList(duplicate(attrfb))
      val emptys = AttrTraverse[F].toList(attrfempty)

      attrfbs.zip(emptys).map {
        case (attrfb, None) => attrfb
        case (_, Some(empty)) => empty.empty[B]
      }

      ???

      // val bs2 = bs.zip(emptys).map {
      //   case (attrfb, None) => attrfb
      //   case (_, Some(empty)) => empty.empty[B]
      // }

      // val zipped: AttrF[(B, Option[Empty[AttrF]])] = unsafeZip2(attrfb, attrfempty)

      // val finalized = zipped.map {
        
      // }

      // attrMap(zipped) {
        
      // }
    }
  }
  object Bound {
    def apply[F[_], G[_]] = new Bound[F, G]{}
  }

  object Test {
    def BindLogicalPlan[A, B](attrfa: Attr[LogicalPlan, A])(f: Attr[LogicalPlan, A] => Attr[LogicalPlan, B]): Attr[LogicalPlan, B] = {
      // As we are descending down the tree, last definition in a let binding wins:
      implicit val last = Semigroup.lastSemigroup[Attr[LogicalPlan, A]]

      type MapSymbol[X] = Map[Symbol, Attr[LogicalPlan, X]]
      type Product[X] = (Attr[LogicalPlan, X], MapSymbol[X])

      type AttrLogicalPlan[X] = Attr[LogicalPlan, X]

      type Subst[X] = Option[(AttrLogicalPlan[X], Empty[AttrLogicalPlan])]

      def empty[A]: MapSymbol[A] = Map()

      Bound[LogicalPlan, MapSymbol].apply[A, B](attrfa)(new NaturalTransformation[AttrLogicalPlan, MapSymbol] {
        def apply[X](plan: Attr[LogicalPlan, X]): MapSymbol[X] = {
          plan.unFix.unAnn.fold[MapSymbol[X]](
            read      = _ => empty,
            constant  = _ => empty,
            join      = (_, _, _, _, _, _) => empty,
            invoke    = (_, _) => empty,
            free      = _ => empty,
            let       = (let, attr) => let
          )
        }
      },
      new NaturalTransformation[Product, Subst] {
        def apply[Y](fa: Product[Y]): Subst[Y] = {
          val (attr, map) = fa

          attr.unFix.unAnn.fold[Subst[Y]](
            read      = _ => None,
            constant  = _ => None,
            join      = (_, _, _, _, _, _) => None,
            invoke    = (_, _) => None,
            free      = symbol => ???, //map.get(symbol).getOrElse(attr),
            let       = (_, _) => None
          )
        }
      })(f)
    }
  }

  sealed trait JoinType
  object JoinType {
    case object Inner extends JoinType
    case object LeftOuter extends JoinType
    case object RightOuter extends JoinType
    case object FullOuter extends JoinType
  }

  sealed trait JoinRel
  object JoinRel {
    case object Eq extends JoinRel
    case object Neq extends JoinRel
    case object Lt extends JoinRel
    case object Lte extends JoinRel
    case object Gt extends JoinRel
    case object Gte extends JoinRel
  }
}

