package slamdata.engine

import scalaz.{Functor, Foldable, Show, Monoid, Traverse, Applicative}

import scalaz.std.list._
import scalaz.std.map._

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

