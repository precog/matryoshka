package slamdata.engine

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.fs.Path

import slamdata.engine.analysis._
import fixplate._

sealed trait LogicalPlan[+A]
object LogicalPlan {
  import slamdata.engine.std.StdLib._
  import structural._

  implicit val LogicalPlanTraverse = new Traverse[LogicalPlan] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan[B]] = {
      fa match {
        case x @ ReadF(_) => G.point(x)
        case x @ ConstantF(_) => G.point(x)
        case JoinF(left, right, tpe, rel, lproj, rproj) =>
          G.apply4(f(left), f(right), f(lproj), f(rproj))(JoinF(_, _, tpe, rel, _, _))
        case InvokeF(func, values) => G.map(Traverse[List].sequence(values.map(f)))(InvokeF(func, _))
        case x @ FreeF(_) => G.point(x)
        case LetF(ident, form0, in0) =>
          G.apply2(f(form0), f(in0))(LetF(ident, _, _))
      }
    }

    override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] = {
      v match {
        case x @ ReadF(_) => x
        case x @ ConstantF(_) => x
        case JoinF(left, right, tpe, rel, lproj, rproj) =>
          JoinF(f(left), f(right), tpe, rel, f(lproj), f(rproj))
        case InvokeF(func, values) => InvokeF(func, values.map(f))
        case x @ FreeF(_) => x
        case LetF(ident, form, in) => LetF(ident, f(form), f(in))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case x @ ReadF(_) => F.zero
        case x @ ConstantF(_) => F.zero
        case JoinF(left, right, tpe, rel, lproj, rproj) =>
          F.append(F.append(f(left), f(right)), F.append(f(lproj), f(rproj)))
        case InvokeF(func, values) => Foldable[List].foldMap(values)(f)
        case x @ FreeF(_) => F.zero
        case LetF(_, form, in) => {
          F.append(f(form), f(in))
        }
      }
    }

    override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case x @ ReadF(_) => z
        case x @ ConstantF(_) => z
        case JoinF(left, right, tpe, rel, lproj, rproj) =>
          f(left, f(right, f(lproj, f(rproj, z))))
        case InvokeF(func, values) => Foldable[List].foldRight(values, z)(f)
        case x @ FreeF(_) => z
        case LetF(ident, form, in) => f(form, f(in, z))
      }
    }
  }
  implicit val RenderTreeLogicalPlan: RenderTree[LogicalPlan[_]] = new RenderTree[LogicalPlan[_]] {
    // Note: these are all terminals; the wrapping Term or Cofree will use these to build nodes with children.
    override def render(v: LogicalPlan[_]) = v match {
      case ReadF(name)                 => Terminal(name.pathname,             List("LogicalPlan", "Read"))
      case ConstantF(data)             => Terminal(data.toString,             List("LogicalPlan", "Constant"))
      case JoinF(_, _, tpe, rel, _, _) => Terminal(tpe.toString + ", " + rel, List("LogicalPlan", "Join"))
      case InvokeF(func, _     )       => Terminal(func.name,                 List("LogicalPlan", "Invoke", func.mappingType.toString))
      case FreeF(name)                 => Terminal(name.toString,             List("LogicalPlan", "Free"))
      case LetF(ident, _, _)           => Terminal(ident.toString,            List("LogicalPlan", "Let"))
    }
  }
  implicit val EqualFLogicalPlan = new fp.EqualF[LogicalPlan] {
    def equal[A](v1: LogicalPlan[A], v2: LogicalPlan[A])(implicit A: Equal[A]): Boolean = (v1, v2) match {
      case (ReadF(n1), ReadF(n2)) => n1 == n2
      case (ConstantF(d1), ConstantF(d2)) => d1 == d2
      case (JoinF(l1, r1, tpe1, rel1, lproj1, rproj1),
            JoinF(l2, r2, tpe2, rel2, lproj2, rproj2)) =>
        A.equal(l1, l2) && A.equal(r1, r2) && A.equal(lproj1, lproj2) && A.equal(rproj1, rproj2) && tpe1 == tpe2
      case (InvokeF(f1, v1), InvokeF(f2, v2)) => Equal[List[A]].equal(v1, v2) && f1 == f2
      case (FreeF(n1), FreeF(n2)) => n1 == n2
      case (LetF(ident1, form1, in1), LetF(ident2, form2, in2)) =>
        ident1 == ident2 && A.equal(form1, form2) && A.equal(in1, in2)
      case _ => false
    }
  }

  case class ReadF(path: Path) extends LogicalPlan[Nothing] {
    override def toString = s"""Read(Path("${path.simplePathname}"))"""
  }
  object Read {
    def apply(path: Path): Term[LogicalPlan] =
      Term[LogicalPlan](new ReadF(path))
  }

  case class ConstantF(data: Data) extends LogicalPlan[Nothing]
  object Constant {
    def apply(data: Data): Term[LogicalPlan] =
      Term[LogicalPlan](ConstantF(data))
  }

  case class JoinF[A](left: A, right: A,
                               joinType: JoinType, joinRel: Mapping,
                               leftProj: A, rightProj: A) extends LogicalPlan[A] {
    override def toString = s"Join($left, $right, $joinType, $joinRel, $leftProj, $rightProj)"
  }
  object Join {
    def apply(left: Term[LogicalPlan], right: Term[LogicalPlan],
               joinType: JoinType, joinRel: Mapping,
               leftProj: Term[LogicalPlan], rightProj: Term[LogicalPlan]): Term[LogicalPlan] =
      Term[LogicalPlan](JoinF(left, right, joinType, joinRel, leftProj, rightProj))
  }

  case class InvokeF[A](func: Func, values: List[A]) extends LogicalPlan[A] {
    override def toString = {
      val funcName = if (func.name(0).isLetter) func.name.split('_').map(_.toLowerCase.capitalize).mkString
                      else "\"" + func.name + "\""
      funcName + "(" + values.mkString(", ") + ")"
    }
  }
  object Invoke {
    def apply(func: Func, values: List[Term[LogicalPlan]]): Term[LogicalPlan] =
      Term[LogicalPlan](InvokeF(func, values))
  }

  case class FreeF(name: Symbol) extends LogicalPlan[Nothing]
  object Free {
    def apply(name: Symbol): Term[LogicalPlan] =
      Term[LogicalPlan](FreeF(name))
  }

  case class LetF[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
  object Let {
    def apply(let: Symbol, form: Term[LogicalPlan], in: Term[LogicalPlan]): Term[LogicalPlan] =
      Term[LogicalPlan](LetF(let, form, in))
  }

  implicit val LogicalPlanBinder: Binder[LogicalPlan, ({type f[A]=Map[Symbol, Cofree[LogicalPlan, A]]})#f] = {
    type CofreeLogicalPlan[X] = Cofree[LogicalPlan, X]

    type MapSymbol[X] = Map[Symbol, CofreeLogicalPlan[X]]

    new Binder[LogicalPlan, MapSymbol] {
      val bindings = new NaturalTransformation[CofreeLogicalPlan, MapSymbol] {
        def empty[A]: MapSymbol[A] = Map()

        def apply[X](plan: Cofree[LogicalPlan, X]): MapSymbol[X] = {
          plan.tail match {
            case LetF(ident, form, _) => Map(ident -> form)
            case _                    => empty
          }
        }
      }

      val subst = new NaturalTransformation[`CofreeF * G`, Subst] {
        def apply[Y](fa: `CofreeF * G`[Y]): Subst[Y] = {
          val (attr, map) = fa

          attr.tail match {
            case FreeF(symbol) =>
              map.get(symbol).map(p =>
                (p, new Forall[Unsubst] {
                  def apply[A] = { (a: A) => attrK(Free(symbol), a) }
                }))
            case _ => None
          }
        }
      }
    }
  }

  def lpBoundPhase[M[_], A, B](phase: PhaseM[M, LogicalPlan, A, B])(implicit M: Functor[M]): PhaseM[M, LogicalPlan, A, B] = {
    type MapSymbol[A] = Map[Symbol, Cofree[LogicalPlan, A]]

    implicit val sg = Semigroup.lastSemigroup[Cofree[LogicalPlan, A]]

    bound[M, LogicalPlan, MapSymbol, A, B](phase)(M, LogicalPlanTraverse, Monoid[MapSymbol[A]], LogicalPlanBinder)
  }

  def lpBoundPhaseS[S, A, B](phase: PhaseS[LogicalPlan, S, A, B]): PhaseS[LogicalPlan, S, A, B] = {
    type St[A] = State[S, A]

    lpBoundPhase[St, A, B](phase)
  }

  def lpBoundPhaseE[E, A, B](phase: PhaseE[LogicalPlan, E, A, B]): PhaseE[LogicalPlan, E, A, B] = {
    type EitherE[A] = E \/ A

    lpBoundPhase[EitherE, A, B](phase)
  }

  /**
   Given a function that does stateful bottom-up annotation, apply it to an
   expression in such a way that each bound expression is evaluated precisely
   once.
   */
  def optimalBoundPhaseM[M[_]: Monad, A, B](f: LogicalPlan[Cofree[LogicalPlan, (A, B)]] => M[B])
                                  (implicit LP: Functor[LogicalPlan]): PhaseM[M, LogicalPlan, A, B] =
    PhaseM[M, LogicalPlan, A, B] {

      def loop(attr: Cofree[LogicalPlan, A], vars: Map[Symbol, Cofree[LogicalPlan, B]]): M[Cofree[LogicalPlan, B]] = {

        def loop0: M[Cofree[LogicalPlan, B]] = for {
          rec <- Traverse[LogicalPlan].sequence(attr.tail.map { (attrA: Cofree[LogicalPlan, A]) =>
            (for {
              attrB <- loop(attrA, vars)
            } yield unsafeZip2(attrA, attrB))
          })

          b <- f(rec)
        } yield Cofree[LogicalPlan, B](b, rec.map(_.map(_._2)))

        attr.tail match {
          case FreeF(name) => vars.get(name).getOrElse(sys.error("not bound: " + name)).point[M] // FIXME: should be surfaced with -\/? See #414
          case LetF(ident, form, in) =>
            for {
              form1 <- loop(form, vars)
              in1   <- loop(in, vars + (ident -> form1))
            } yield Cofree(in1.head, LetF(ident, form1, in1))
          case _ => loop0
        }
      }

      loop(_, Map())
    }

  def optimalBoundPhaseS[S, A, B](f: LogicalPlan[Cofree[LogicalPlan, (A, B)]] => State[S, B])(implicit LP: Functor[LogicalPlan]): PhaseS[LogicalPlan, S, A, B] = {
      type St[B] = State[S, B]

      optimalBoundPhaseM[St, A, B](f)
    }

  def optimalBoundSynthPara2PhaseM[M[_]: Monad, A, B](f: LogicalPlan[(Term[LogicalPlan], B)] => M[B]): PhaseM[M, LogicalPlan, A, B] = {
    val f0: (LogicalPlan[Cofree[LogicalPlan, (A, B)]] => M[B]) =
      lp => f(lp.map(attr => forget(attr) -> attr.head._2))

    optimalBoundPhaseM(f0)
  }

  def optimalBoundSynthPara2Phase[A, B](f: LogicalPlan[(Term[LogicalPlan], B)] => B): Phase[LogicalPlan, A, B] =
    optimalBoundSynthPara2PhaseM[IdInstances#Id, A, B](f)

  sealed trait JoinType
  object JoinType {
    case object Inner extends JoinType
    case object LeftOuter extends JoinType
    case object RightOuter extends JoinType
    case object FullOuter extends JoinType
  }
}

