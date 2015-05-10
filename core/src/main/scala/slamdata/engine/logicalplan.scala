package slamdata.engine

import collection.immutable.Map

import scalaz._
import Scalaz._

import slamdata.engine.fp._
import slamdata.engine.fs.Path

import slamdata.engine.analysis._
import fixplate._

sealed trait LogicalPlan[+A]
object LogicalPlan {
  import slamdata.engine.std.StdLib._
  import identity._
  import set._
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
    val nodeType = "LogicalPlan" :: Nil

    // Note: these are all terminals; the wrapping Term or Cofree will use these to build nodes with children.
    override def render(v: LogicalPlan[_]) = v match {
      case ReadF(name)                 => Terminal("Read" :: nodeType, Some(name.pathname))
      case ConstantF(data)             => Terminal("Constant" :: nodeType, Some(data.toString))
      case JoinF(_, _, tpe, rel, _, _) => Terminal("Join" :: nodeType, Some(tpe.toString + ", " + rel))
      case InvokeF(func, _     )       => Terminal(func.mappingType.toString :: "Invoke" :: nodeType, Some(func.name))
      case FreeF(name)                 => Terminal("Free" :: nodeType, Some(name.toString))
      case LetF(ident, _, _)           => Terminal("Let" :: nodeType, Some(ident.toString))
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

  final case class ReadF(path: Path) extends LogicalPlan[Nothing] {
    override def toString = s"""Read(Path("${path.simplePathname}"))"""
  }
  object Read {
    def apply(path: Path): Term[LogicalPlan] =
      Term[LogicalPlan](new ReadF(path))
  }

  final case class ConstantF(data: Data) extends LogicalPlan[Nothing]
  object Constant {
    def apply(data: Data): Term[LogicalPlan] =
      Term[LogicalPlan](ConstantF(data))
  }

  final case class JoinF[A](left: A, right: A,
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

  final case class InvokeF[A](func: Func, values: List[A]) extends LogicalPlan[A] {
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

  final case class FreeF(name: Symbol) extends LogicalPlan[Nothing]
  object Free {
    def apply(name: Symbol): Term[LogicalPlan] =
      Term[LogicalPlan](FreeF(name))
  }

  final case class LetF[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]
  object Let {
    def apply(let: Symbol, form: Term[LogicalPlan], in: Term[LogicalPlan]): Term[LogicalPlan] =
      Term[LogicalPlan](LetF(let, form, in))
  }

  implicit val LogicalPlanUnzip = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val LogicalPlanBinder = new Binder[LogicalPlan] {
      type G[A] = Map[Symbol, A]

      def initial[A] = Map[Symbol, A]()

      def bindings[A](t: LogicalPlan[Term[LogicalPlan]], b: G[A])(f: LogicalPlan[Term[LogicalPlan]] => A): G[A] =
        t match {
          case LetF(ident, form, _) => b + (ident -> f(form.unFix))
          case _                    => b
        }

      def subst[A](t: LogicalPlan[Term[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case FreeF(symbol) => b.get(symbol)
          case _             => None
        }
    }

  val namesƒ: LogicalPlan[Set[Symbol]] => Set[Symbol] = {
    case FreeF(name) => Set(name)
    case x           => x.fold
  }

  def freshName[F[_]: Functor: Foldable](
    prefix: String, plans: F[Term[LogicalPlan]]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  val shapeƒ: LogicalPlan[(Term[LogicalPlan], Option[List[Term[LogicalPlan]]])] => Option[List[Term[LogicalPlan]]] = {
    case JoinF(left, right, _, _, _, _) =>
      List(left._2, right._2).sequence.map(_.flatten)
    case LetF(_, _, body) => body._2
    case ConstantF(Data.Obj(map)) =>
      Some(map.keys.map(n => Constant(Data.Str(n))).toList)
    case InvokeF(DeleteField, List(src, field)) =>
      src._2.map(_.filterNot(_ == field._1))
    case InvokeF(MakeObject, List(field, src)) => Some(List(field._1))
    case InvokeF(ObjectConcat, srcs) => srcs.map(_._2).sequence.map(_.flatten)
    // NB: the remaining InvokeF cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeF(OrderBy, List(src, _, _)) => src._2
    case InvokeF(Take, List(src, _)) => src._2
    case InvokeF(Drop, List(src, _)) => src._2
    case InvokeF(Filter, List(src, _)) => src._2
    case InvokeF(Cross, srcs) => srcs.map(_._2).sequence.map(_.flatten)
    case InvokeF(GroupBy, List(src, _)) => src._2
    case InvokeF(Distinct, List(src, _)) => src._2
    case InvokeF(DistinctBy, List(src, _)) => src._2
    case InvokeF(Squash, List(src)) => src._2
    case _ => None
  }

  // TODO: Generalize this to Binder
  def lpParaZygoHistoM[M[_]: Monad, A, B](
    t: Term[LogicalPlan])(
    f: LogicalPlan[(Term[LogicalPlan], B)] => B,
    g: LogicalPlan[(B, Cofree[LogicalPlan, A])] => M[A]):
      M[A] = {
    def loop(t: Term[LogicalPlan], bind: Map[Symbol, ((B, A), Cofree[LogicalPlan, A])]):
        M[((B, A), Cofree[LogicalPlan, A])] = {
      lazy val default: M[((B, A), Cofree[LogicalPlan, A])] = for {
        tup <- (t.unFix.map { x => for {
          tup <- loop(x, bind)
          ((b, a), coa) = tup
        } yield (((x, b), (b, coa)), coa)
        }).sequence
        (ba, coa) = tup.unfzip
        (b, a) = ba.unfzip.bimap(f, g)
        a0 <- a
      } yield ((b, a0), Cofree(a0, coa))

      t.unFix match {
        case FreeF(name)            => bind.get(name).fold(default)(_.point[M])
        case LetF(name, form, body) => for {
          form1 <- loop(form, bind)
          rez   <- loop(body, bind + (name -> form1))
        } yield rez
        case _                      => default
      }
    }

    for {
      rez <- loop(t, Map())
    } yield rez._1._2
  }

  def lpParaZygoHistoS[S, A, B] = lpParaZygoHistoM[State[S, ?], A, B] _
  def lpParaZygoHisto[A, B] = lpParaZygoHistoM[Id, A, B] _

  sealed trait JoinType
  object JoinType {
    final case object Inner extends JoinType
    final case object LeftOuter extends JoinType
    final case object RightOuter extends JoinType
    final case object FullOuter extends JoinType
  }
}
