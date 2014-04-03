package slamdata.engine

import scalaz.{Functor, Foldable, Show, Monoid, Traverse, Applicative}

import scalaz.std.list._

sealed trait LogicalPlan

object LogicalPlan {
  case class Read(resource: String) extends LogicalPlan

  case class Constant(data: Data) extends LogicalPlan

  case class Filter(input: LogicalPlan, predicate: LogicalPlan) extends LogicalPlan

  case class Join(left: LogicalPlan, right: LogicalPlan, 
                  joinType: JoinType, joinRel: JoinRel, 
                  leftProj: Lambda, rightProj: Lambda) extends LogicalPlan

  case class Cross(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan

  case class Invoke(func: Func, values: List[LogicalPlan]) extends LogicalPlan

  case class Free(name: String) extends LogicalPlan

  case class Lambda(name: String, value: LogicalPlan) extends LogicalPlan

  case class Sort(value: LogicalPlan, by: LogicalPlan) extends LogicalPlan

  case class Group(value: LogicalPlan, by: LogicalPlan) extends LogicalPlan

  case class Take(value: LogicalPlan, count: Long) extends LogicalPlan

  case class Drop(value: LogicalPlan, count: Long) extends LogicalPlan

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


sealed trait LogicalPlan2[+A] {
  import LogicalPlan2._

  def fold[Z](
      read:       String  => Z, 
      constant:   Data    => Z,
      free:       String  => Z,
      join:       (A, A, JoinType, JoinRel, A, A) => Z,
      invoke:     (Func, List[A]) => Z,
      fmap:       (A, Lambda[A]) => Z,
      group:      (A, A) => Z
    ): Z = this match {
    case Read(x)              => read(x)
    case Constant(x)          => constant(x)
    case Free(x)              => free(x)
    case Join(left, right, tpe, rel, lproj, rproj) => join(left, right, tpe, rel, lproj, rproj)
    case Invoke(func, values) => invoke(func, values)
    case FMap(value, lambda)  => fmap(value, lambda)
    case Group(value, by)     => group(value, by)
  }
}

object LogicalPlan2 {
  implicit val LogicalPlanFunctor = new Functor[LogicalPlan2] with Foldable[LogicalPlan2] with Traverse[LogicalPlan2] {
    def traverseImpl[G[_], A, B](fa: LogicalPlan2[A])(f: A => G[B])(implicit G: Applicative[G]): G[LogicalPlan2[B]] = {
      fa match {
        case x @ Read(_) => G.point(x)
        case x @ Constant(_) => G.point(x)
        case x @ Free(_) => G.point(x)
        case Join(left, right, tpe, rel, lproj, rproj) => 
          G.apply4(f(left), f(right), f(lproj), f(rproj))(Join(_, _, tpe, rel, _, _))
        case Invoke(func, values) => G.map(Traverse[List].sequence(values.map(f)))(Invoke(func, _))
        case FMap(value, lambda) => G.apply2(f(value), f(lambda.value))((v, l) => FMap(v, Lambda(lambda.name, l)))
        case Group(value, by) => G.apply2(f(value), f(by))(Group(_, _))
      }
    }

    override def map[A, B](v: LogicalPlan2[A])(f: A => B): LogicalPlan2[B] = {
      v match {
        case x @ Read(_) => x
        case x @ Constant(_) => x
        case x @ Free(_) => x
        case Join(left, right, tpe, rel, lproj, rproj) =>
          Join(f(left), f(right), tpe, rel, f(lproj), f(rproj))
        case Invoke(func, values) => Invoke(func, values.map(f))
        case FMap(value, lambda) => FMap(f(value), Lambda(lambda.name, f(lambda.value)))
        case Group(value, by) => Group(f(value), f(by))
      }
    }

    override def foldMap[A, B](fa: LogicalPlan2[A])(f: A => B)(implicit F: Monoid[B]): B = {
      fa match {
        case x @ Read(_) => F.zero
        case x @ Constant(_) => F.zero
        case x @ Free(_) => F.zero
        case Join(left, right, tpe, rel, lproj, rproj) =>
          F.append(F.append(f(left), f(right)), F.append(f(lproj), f(rproj)))
        case Invoke(func, values) => Foldable[List].foldMap(values)(f)
        case FMap(value, lambda) => F.append(f(value), f(lambda.value))
        case Group(value, by) => F.append(f(value), f(by))
      }
    }

    override def foldRight[A, B](fa: LogicalPlan2[A], z: => B)(f: (A, => B) => B): B = {
      fa match {
        case x @ Read(_) => z
        case x @ Constant(_) => z
        case x @ Free(_) => z
        case Join(left, right, tpe, rel, lproj, rproj) =>
          f(left, f(right, f(lproj, f(rproj, z))))
        case Invoke(func, values) => Foldable[List].foldRight(values, z)(f)
        case FMap(value, lambda) => f(value, f(lambda.value, z))
        case Group(value, by) => f(value, f(by, z))
      }
    }
  }
  case class Read(resource: String) extends LogicalPlan2[Nothing]

  case class Constant(data: Data) extends LogicalPlan2[Nothing]

  case class Join[A](left: A, right: A, 
                     joinType: JoinType, joinRel: JoinRel, 
                     leftProj: A, rightProj: A) extends LogicalPlan2[A]

  case class Invoke[A](func: Func, values: List[A]) extends LogicalPlan2[A]

  case class FMap[A](value: A, lambda: Lambda[A]) extends LogicalPlan2[A]

  case class Lambda[+A](name: String, value: A)

  case class Free(name: String) extends LogicalPlan2[Nothing]

  case class Group[A](value: A, by: A) extends LogicalPlan2[A]

  import slamdata.engine.analysis._
  import fixplate._

  type LPTerm = Term[LogicalPlan2]

  type LP = LogicalPlan2[LPTerm]

  type LPAttr[A] = Attr[LogicalPlan2, A]

  type LPPhase[A, B] = Phase[LogicalPlan2, A, B]

  def read(resource: String): LPTerm = Term[LogicalPlan2](Read(resource))
  def constant(data: Data): LPTerm = Term[LogicalPlan2](Constant(data))
  def join(left: LP, right: LP, joinType: JoinType, joinRel: JoinRel, leftProj: LP, rightProj: LP): LPTerm = 
    Term(Join(Term(left), Term(right), joinType, joinRel, Term(leftProj), Term(rightProj)))
  def invoke(func: Func, values: List[LP]): LPTerm = Term(Invoke(func, values.map(Term.apply)))
  def fmap(value: LP, lambda: Lambda[LP]): LPTerm = Term(FMap(Term(value), Lambda(lambda.name, Term(lambda.value))))
  def free(name: String): LPTerm = Term[LogicalPlan2](Free(name))
  def group(value: LP, by: LP): LPTerm = Term(Group(Term(value), Term(by)))

  val a1: Attr[LogicalPlan2, Int] = synthetize(read("foo")) { (input: LogicalPlan2[Int]) =>
    1 + Foldable[LogicalPlan2].foldLeft(input, 0)(_ + _)
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

