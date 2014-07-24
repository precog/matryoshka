package slamdata.engine

import scalaz._

import scalaz.std.string._
import scalaz.std.list._
import scalaz.std.map._

import slamdata.engine.fp._
import slamdata.engine.fs.Path

import slamdata.engine.analysis._
import fixplate._


sealed trait LogicalPlan[+A] {
  import LogicalPlan._

  def fold[Z](
      read:       Path  => Z, 
      constant:   Data  => Z,
      join:       (A, A, JoinType, JoinRel, A, A) => Z,
      invoke:     (Func, List[A]) => Z,
      free:       Symbol  => Z,
      let:        (Symbol, A, A) => Z
    ): Z = this match {
    case Read0(x)              => read(x)
    case Constant0(x)          => constant(x)
    case Join0(left, right, 
              tpe, rel, 
              lproj, rproj)    => join(left, right, tpe, rel, lproj, rproj)
    case Invoke0(func, values) => invoke(func, values)
    case Free0(name)           => free(name)
    case Let0(ident, form, in) => let(ident, form, in)
  }
}

object LogicalPlan {
  implicit val LogicalPlanTraverse = new Traverse[LogicalPlan] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan[B]] = {
      fa match {
        case x @ Read0(_) => G.point(x)
        case x @ Constant0(_) => G.point(x)
        case Join0(left, right, tpe, rel, lproj, rproj) => 
          G.apply4(f(left), f(right), f(lproj), f(rproj))(Join0(_, _, tpe, rel, _, _))
        case Invoke0(func, values) => G.map(Traverse[List].sequence(values.map(f)))(Invoke0(func, _))
        case x @ Free0(_) => G.point(x)
        case Let0(ident, form0, in0) =>
          G.apply2(f(form0), f(in0))(Let0(ident, _, _))
      }
    }

    override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] = {
      v match {
        case x @ Read0(_) => x
        case x @ Constant0(_) => x
        case Join0(left, right, tpe, rel, lproj, rproj) =>
          Join0(f(left), f(right), tpe, rel, f(lproj), f(rproj))
        case Invoke0(func, values) => Invoke0(func, values.map(f))
        case x @ Free0(_) => x
        case Let0(ident, form, in) => Let0(ident, f(form), f(in))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case x @ Read0(_) => F.zero
        case x @ Constant0(_) => F.zero
        case Join0(left, right, tpe, rel, lproj, rproj) =>
          F.append(F.append(f(left), f(right)), F.append(f(lproj), f(rproj)))
        case Invoke0(func, values) => Foldable[List].foldMap(values)(f)
        case x @ Free0(_) => F.zero
        case Let0(_, form, in) => {
          F.append(f(form), f(in))
        }
      }
    }

    override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case x @ Read0(_) => z
        case x @ Constant0(_) => z
        case Join0(left, right, tpe, rel, lproj, rproj) =>
          f(left, f(right, f(lproj, f(rproj, z))))
        case Invoke0(func, values) => Foldable[List].foldRight(values, z)(f)
        case x @ Free0(_) => z
        case Let0(ident, form, in) => f(form, f(in, z))
      }
    }
  }
  implicit val RenderTreeLogicalPlan: RenderTree[LogicalPlan[_]] = new RenderTree[LogicalPlan[_]] {
    // Note: these are all terminals; the wrapping Term or Attr will use these to build nodes with children.
    override def render(v: LogicalPlan[_]) = v match {
      case Read0(name)                 => Terminal(name.pathname,     List("LogicalPlan", "Read"))
      case Constant0(data)             => Terminal(data.toString,     List("LogicalPlan", "Constant"))
      case Join0(_, _, tpe, rel, _, _) => Terminal(tpe + ", " + rel , List("LogicalPlan", "Join"))
      case Invoke0(func, values)       => Terminal(func.name,         List("LogicalPlan", "Invoke", func.mappingType.toString))
      case Free0(name)                 => Terminal(name.toString,     List("LogicalPlan", "Free"))
      case Let0(ident, form, in)       => Terminal(ident.toString,    List("LogicalPlan", "Let"))
    }
  }
  implicit val EqualFLogicalPlan = new fp.EqualF[LogicalPlan] {
    def equal[A](v1: LogicalPlan[A], v2: LogicalPlan[A])(implicit A: Equal[A]): Boolean = (v1, v2) match {
      case (Read0(n1), Read0(n2)) => n1 == n2
      case (Constant0(d1), Constant0(d2)) => d1 == d2
      case (Join0(l1, r1, tpe1, rel1, lproj1, rproj1), 
            Join0(l2, r2, tpe2, rel2, lproj2, rproj2)) => 
        A.equal(l1, l2) && A.equal(r1, r2) && A.equal(lproj1, lproj2) && A.equal(rproj1, rproj2) && tpe1 == tpe2
      case (Invoke0(f1, v1), Invoke0(f2, v2)) => Equal[List[A]].equal(v1, v2) && f1 == f2
      case (Free0(n1), Free0(n2)) => n1 == n2
      case (Let0(ident1, form1, in1), Let0(ident2, form2, in2)) =>
        ident1 == ident2 && A.equal(form1, form2) && A.equal(in1, in2)
      case _ => false
    }
  }

  import slamdata.engine.analysis.fixplate.{Attr => FAttr}

  private case class Read0(path: Path) extends LogicalPlan[Nothing]
  object Read {
    def apply(path: Path): Term[LogicalPlan] = 
      Term[LogicalPlan](new Read0(path))
    
    def unapply(t: Term[LogicalPlan]): Option[Path] = 
      t.unFix match {
        case Read0(path) => Some(path)
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[Path] = Read.unapply(forget(a))
    }
  }
  
  private case class Constant0(data: Data) extends LogicalPlan[Nothing]
  object Constant {
    def apply(data: Data): Term[LogicalPlan] = 
      Term[LogicalPlan](Constant0(data))

    def unapply(t: Term[LogicalPlan]): Option[Data] = 
      t.unFix match {
        case Constant0(data) => Some(data)
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[Data] = Constant.unapply(forget(a))
    }
  }

  private case class Join0[A](left: A, right: A, 
                               joinType: JoinType, joinRel: JoinRel, 
                               leftProj: A, rightProj: A) extends LogicalPlan[A]
  object Join {
    def apply(left: Term[LogicalPlan], right: Term[LogicalPlan], 
               joinType: JoinType, joinRel: JoinRel, 
               leftProj: Term[LogicalPlan], rightProj: Term[LogicalPlan]): Term[LogicalPlan] = 
      Term[LogicalPlan](Join0(left, right, joinType, joinRel, leftProj, rightProj))

    def unapply(t: Term[LogicalPlan]): Option[(Term[LogicalPlan], Term[LogicalPlan], JoinType, JoinRel, Term[LogicalPlan], Term[LogicalPlan])] = 
      t.unFix match {
        case Join0(left, right, joinType, joinRel, leftProj, rightProj) => Some((left, right, joinType, joinRel, leftProj, rightProj))
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[(FAttr[LogicalPlan, A], FAttr[LogicalPlan, A], JoinType, JoinRel, FAttr[LogicalPlan, A], FAttr[LogicalPlan, A])] = 
        a.unFix.unAnn match {
          case Join0(left, right, joinType, joinRel, leftProj, rightProj) => Some((left, right, joinType, joinRel, leftProj, rightProj))
          case _ => None
        }
    }
  }

  private case class Invoke0[A](func: Func, values: List[A]) extends LogicalPlan[A]
  object Invoke {
    def apply(func: Func, values: List[Term[LogicalPlan]]): Term[LogicalPlan] = 
      Term[LogicalPlan](Invoke0(func, values))

    def unapply(t: Term[LogicalPlan]): Option[(Func, List[Term[LogicalPlan]])] = 
      t.unFix match {
        case Invoke0(func, values) => Some((func, values))
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[(Func, List[FAttr[LogicalPlan, A]])] = 
        a.unFix.unAnn match {
          case Invoke0(func, values) => Some((func, values))
          case _ => None
        }
    }
  }

  private case class Free0(name: Symbol) extends LogicalPlan[Nothing]
  object Free {
    def apply(name: Symbol): Term[LogicalPlan] = 
      Term[LogicalPlan](Free0(name))
    
    def unapply(t: Term[LogicalPlan]): Option[Symbol] = 
      t.unFix match {
        case Free0(name) => Some(name)
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[Symbol] = Free.unapply(forget(a))
    }
  }

  private case class Let0[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
  object Let {
    def apply(let: Symbol, form: Term[LogicalPlan], in: Term[LogicalPlan]): Term[LogicalPlan] = 
      Term[LogicalPlan](Let0(let, form, in))
    
    def unapply(t: Term[LogicalPlan]): Option[(Symbol, Term[LogicalPlan], Term[LogicalPlan])] = 
      t.unFix match {
        case Let0(let, form, in) => Some((let, form, in))
        case _ => None
      }

    object Attr {
      def unapply[A](a: FAttr[LogicalPlan, A]): Option[(Symbol, FAttr[LogicalPlan, A], FAttr[LogicalPlan, A])] = 
        a.unFix.unAnn match {
          case Let0(let, form, in) => Some((let, form, in))
          case _ => None
        }
    }
  }

  implicit val LogicalPlanBinder: Binder[LogicalPlan, ({type f[A]=Map[Symbol, Attr[LogicalPlan, A]]})#f] = {
    type AttrLogicalPlan[X] = Attr[LogicalPlan, X]

    type MapSymbol[X] = Map[Symbol, AttrLogicalPlan[X]]    

    new Binder[LogicalPlan, MapSymbol] {
      val bindings = new NaturalTransformation[AttrLogicalPlan, MapSymbol] {
        def empty[A]: MapSymbol[A] = Map()

        def apply[X](plan: Attr[LogicalPlan, X]): MapSymbol[X] = {
          plan.unFix.unAnn.fold[MapSymbol[X]](
            read      = _ => empty,
            constant  = _ => empty,
            join      = (_, _, _, _, _, _) => empty,
            invoke    = (_, _) => empty,
            free      = _ => empty,
            let       = (ident, form, _) => Map(ident -> form)
          )
        }
      }

      val subst = new NaturalTransformation[`AttrF * G`, Subst] {
        def apply[Y](fa: `AttrF * G`[Y]): Subst[Y] = {
          val (attr, map) = fa

          attr.unFix.unAnn.fold[Subst[Y]](
            read      = _ => None,
            constant  = _ => None,
            join      = (_, _, _, _, _, _) => None,
            invoke    = (_, _) => None,
            free      = symbol => map.get(symbol).map(p => (p, new Forall[Unsubst] { def apply[A] = { (a: A) => attrK(Free(symbol), a) } })),
            let       = (_, _, _) => None
          )
        }
      }
    }
  }

  def lpBoundPhase[M[_], A, B](phase: PhaseM[M, LogicalPlan, A, B])(implicit M: Functor[M]): PhaseM[M, LogicalPlan, A, B] = {
    type MapSymbol[A] = Map[Symbol, Attr[LogicalPlan, A]]

    implicit val sg = Semigroup.lastSemigroup[Attr[LogicalPlan, A]]

    bound[M, LogicalPlan, MapSymbol, A, B](phase)(M, LogicalPlanTraverse, Monoid[MapSymbol[A]], LogicalPlanBinder)
  }

  def lpBoundPhaseE[E, A, B](phase: PhaseE[LogicalPlan, E, A, B]): PhaseE[LogicalPlan, E, A, B] = {
    type EitherE[A] = E \/ A

    lpBoundPhase[EitherE, A, B](phase)
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

