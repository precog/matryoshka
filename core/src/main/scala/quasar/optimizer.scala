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
import quasar.namegen._
import quasar.recursionschemes._, Fix._, Recursive.ops._, FunctorT.ops._, TraverseT.ownOps._

import scalaz._, Scalaz._

object Optimizer {
  import LogicalPlan._
  import quasar.std.StdLib._
  import set._
  import structural._
  import Planner._

  private def countUsageƒ(target: Symbol): LogicalPlan[Int] => Int = {
    case FreeF(symbol) if symbol == target => 1
    case LetF(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inlineƒ[T[_[_]], A](target: Symbol, repl: LogicalPlan[T[LogicalPlan]]):
      LogicalPlan[(T[LogicalPlan], T[LogicalPlan])] => LogicalPlan[T[LogicalPlan]] =
    {
      case FreeF(symbol) if symbol == target => repl
      case LetF(ident, form, body) if ident == target =>
        LetF(ident, form._2, body._1)
      case x => x.map(_._2)
    }

  def simplifyƒ[T[_[_]]: Recursive: FunctorT]:
      LogicalPlan[T[LogicalPlan]] => Option[LogicalPlan[T[LogicalPlan]]] = {
    case inv @ InvokeF(func, _) => func.simplify(inv)
    case LetF(ident, form, in) => form.project match {
      case ConstantF(_) => in.transPara(inlineƒ(ident, form.project)).project.some
      case _ => in.cata(countUsageƒ(ident)) match {
        case 0 => in.project.some
        case 1 => in.transPara(inlineƒ(ident, form.project)).project.some
        case _ => None
      }
    }
    case _ => None
  }

  def simplify(t: Fix[LogicalPlan]): Fix[LogicalPlan] = t.transCata(repeatedly(simplifyƒ))

  val namesƒ: LogicalPlan[Set[Symbol]] => Set[Symbol] = {
    case FreeF(name) => Set(name)
    case x           => x.fold
  }

  def uniqueName[F[_]: Functor: Foldable](
    prefix: String, plans: F[Fix[LogicalPlan]]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  val shapeƒ: LogicalPlan[(Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]])] => Option[List[Fix[LogicalPlan]]] = {
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
    case InvokeF(InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin, _)
        => Some(List(Constant(Data.Str("left")), Constant(Data.Str("right"))))
    case InvokeF(GroupBy, List(src, _)) => src._2
    case InvokeF(Distinct, List(src, _)) => src._2
    case InvokeF(DistinctBy, List(src, _)) => src._2
    case InvokeF(identity.Squash, List(src)) => src._2
    case _ => None
  }

  def preserveFree0[A](x: (Fix[LogicalPlan], A))(f: A => Fix[LogicalPlan]):
      Fix[LogicalPlan] = x._1.unFix match {
    case FreeF(_) => x._1
    case _        => f(x._2)
  }

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      LogicalPlan[(
        Fix[LogicalPlan],
        (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]))] =>
  (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]) = { node =>

    def preserveFree(x: (Fix[LogicalPlan], (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]))) =
      preserveFree0(x)(_._1)

    (node match {
      case InvokeF(DeleteField, List(src, field)) =>
        src._2._2.fold(
          Invoke(DeleteField, List(preserveFree(src), preserveFree(field)))) {
          fields =>
            val name = uniqueName("src", fields)
              Let(name, preserveFree(src),
                Fix(MakeObjectN(fields.filterNot(_ == field._2._1).map(f =>
                  f -> Invoke(ObjectProject, List(Free(name), f))): _*)))
        }
      case lp => Fix(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Fix[LogicalPlan]): Fix[LogicalPlan] =
    t.boundPara(preferProjectionsƒ)._1.transCata(repeatedly(simplifyƒ))

  val elideTypeCheckƒ: LogicalPlan[Fix[LogicalPlan]] => Fix[LogicalPlan] = {
    case LetF(n, b, Fix(TypecheckF(Fix(FreeF(nf)), _, cont, _)))
        if n == nf =>
      Let(n, b, cont)
    case x => Fix(x)
  }

  /** To be used by backends that require collections to contain Obj, this
    * looks at type checks on `Read` then either eliminates them if they are
    * trivial, leaves them if they check field contents, or errors if they are
    * incompatible.
    */
  def assumeReadObjƒ:
      LogicalPlan[Fix[LogicalPlan]] => PlannerError \/ Fix[LogicalPlan] = {
    case x @ LetF(n, r @ Fix(ReadF(_)),
      Fix(TypecheckF(Fix(FreeF(nf)), typ, cont, _)))
        if n == nf =>
      typ match {
        case Type.Obj(m, Some(Type.Top)) if m == ListMap() =>
          \/-(Let(n, r, cont))
        case Type.Obj(_, _) =>
          \/-(Fix(x))
        case _ =>
          -\/(UnsupportedPlan(x,
            Some("collections can only contain objects, but a(n) " +
              typ +
              " is expected")))
      }
    case x => \/-(Fix(x))
  }

  /** Rewrite joins and subsequent filtering so that:
    * 1) Filtering that is equivalent to an equi-join is rewritten into the join condition.
    * 2) Filtering that refers to only side of the join is hoisted prior to the join.
    * The input plan must have been simplified already so that the structure
    * is in a canonical form for inspection.
    */
  val rewriteCrossJoinsƒ: LogicalPlan[(Fix[LogicalPlan], Fix[LogicalPlan])] => State[NameGen, Fix[LogicalPlan]] = { node =>
    import quasar.fp._

    def preserveFree(x: (Fix[LogicalPlan], Fix[LogicalPlan])) = preserveFree0(x)(ι)

    def flattenAnd: Fix[LogicalPlan] => List[Fix[LogicalPlan]] = {
      case Fix(relations.And(ts)) => ts.flatMap(flattenAnd)
      case t                      => List(t)
    }

    sealed trait Component[A]
    // A condition that refers to left and right sources using equality, so may
    // be rewritten into the join condition:
    final case class EquiCond[A](run: (Fix[LogicalPlan], Fix[LogicalPlan]) => A) extends Component[A]
    // A condition which refers only to the left source:
    final case class LeftCond[A](run: Fix[LogicalPlan] => A) extends Component[A]
    // A condition which refers only to the right source:
    final case class RightCond[A](run: Fix[LogicalPlan] => A) extends Component[A]
    // A condition which refers to both sources but doesn't have the right shape
    // to become the join condition:
    final case class OtherCond[A](run: (Fix[LogicalPlan], Fix[LogicalPlan]) => A) extends Component[A]
    // An expression that doesn't refer to any source.
    final case class NeitherCond[A](run: A) extends Component[A]

    implicit val ComponentFunctor = new Functor[Component] {
      def map[A, B](fa: Component[A])(f: A => B) = fa match {
        case EquiCond(run)    => EquiCond((l, r) => f(run(l, r)))
        case LeftCond(run)    => LeftCond((l) => f(run(l)))
        case RightCond(run)   => RightCond((r) => f(run(r)))
        case OtherCond(run)   => OtherCond((l, r) => f(run(l, r)))
        case NeitherCond(run) => NeitherCond(f(run))
      }
    }

    def toComp(left: Fix[LogicalPlan], right: Fix[LogicalPlan])(c: Fix[LogicalPlan]):
        Component[Fix[LogicalPlan]] = {
      c.para[Component[Fix[LogicalPlan]]] {
        case t if t.map(_._1) ≟ left.unFix  => LeftCond(ι)
        case t if t.map(_._1) ≟ right.unFix => RightCond(ι)

        case relations.Eq((_, LeftCond(lc)) :: (_, RightCond(rc)) :: Nil) =>
          EquiCond((l, r) => Fix(relations.Eq(lc(l), rc(r))))
        case relations.Eq((_, RightCond(rc)) :: (_, LeftCond(lc)) :: Nil) =>
          EquiCond((l, r) => Fix(relations.Eq(rc(r), lc(l))))

        case InvokeF(func, ts) =>
          ts.map(_._2).foldRight[Component[List[Fix[LogicalPlan]]]](NeitherCond(Nil)) {
            case (NeitherCond(h), NeitherCond(cs)) => NeitherCond(h :: cs)

            case (LeftCond(h),    NeitherCond(cs)) => LeftCond(t => h(t) :: cs)
            case (LeftCond(h),    LeftCond(cs))    => LeftCond(t => h(t) :: cs(t))

            case (RightCond(h),   NeitherCond(cs)) => RightCond(t => h(t) :: cs)
            case (RightCond(h),   RightCond(cs))   => RightCond(t => h(t) :: cs(t))

            case (LeftCond(h),    RightCond(cs))   => OtherCond((l, r) => h(l) :: cs(r))
            case (RightCond(h),   LeftCond(cs))    => OtherCond((l, r) => h(r) :: cs(l))
          }.map(ts => Fix(InvokeF(func, ts)))

        case t => NeitherCond(Fix(t.map(_._1)))
      }
    }

    def assembleCond(conds: List[Fix[LogicalPlan]]): Fix[LogicalPlan] =
      conds.foldLeft(Constant(Data.True))((acc, c) => Fix(relations.And(acc, c)))

    def newJoin(lSrc: Fix[LogicalPlan], rSrc: Fix[LogicalPlan], comps: List[Component[Fix[LogicalPlan]]]):
        State[NameGen, Fix[LogicalPlan]] = {
      val equis    = comps.collect { case c @ EquiCond(_) => c }
      val lefts    = comps.collect { case c @ LeftCond(_) => c }
      val rights   = comps.collect { case c @ RightCond(_) => c }
      val others   = comps.collect { case c @ OtherCond(_) => c }
      val neithers = comps.collect { case c @ NeitherCond(_) => c }

      for {
        lName  <- freshName("leftSrc")
        lFName <- freshName("left")
        rName  <- freshName("rightSrc")
        rFName <- freshName("right")
        jName  <- freshName("joined")
      } yield {
        // NB: simplifying eagerly to make matching easier up the tree
        simplify(
          Let(lName, lSrc,
            Let(lFName, Fix(Filter(Free(lName), assembleCond(lefts.map(_.run(Free(lName)))))),
              Let(rName, rSrc,
                Let(rFName, Fix(Filter(Free(rName), assembleCond(rights.map(_.run(Free(rName)))))),
                  Let(jName,
                    Fix(InnerJoin(Free(lFName), Free(rFName),
                      assembleCond(equis.map(_.run(Free(lFName), Free(rFName)))))),
                    Fix(Filter(Free(jName), assembleCond(
                      others.map(_.run(JoinDir.Left.projectFrom(Free(jName)), JoinDir.Right.projectFrom(Free(jName)))) ++
                      neithers.map(_.run))))))))))
      }
    }


    node match {
      case Filter((src, Fix(InnerJoin(joinL :: joinR :: joinCond :: Nil))) :: (cond, _) :: Nil) =>
        val comps = flattenAnd(joinCond).map(toComp(joinL, joinR)) ++
                    flattenAnd(cond).map(toComp(JoinDir.Left.projectFrom(src), JoinDir.Right.projectFrom(src)))
        newJoin(joinL, joinR, comps)

      // TODO: enable this when it can be made to not conflict (see tests pending SD-1190).
      // Currently, the rewritten join/filter's shape is not matched by the other case,
      // resulting in un-collapsed Filter steps.
      // case InnerJoin((srcL, _) :: (srcR, _) :: (_, joinCond) :: Nil) =>
      //   val comps = flattenAnd(joinCond).map(toComp(srcL, srcR))
      //   newJoin(srcL, srcR, comps)

      case _ => State.state(Fix(node.map(preserveFree)))
    }
  }

  /** Apply universal, type-oblivious transformations intended to
    * improve the performance of a query regardless of the backend. The
    * input is expected to come straight from the SQL^2 compiler or
    * another source of un-optimized queries.
    */
  def optimize(t: Fix[LogicalPlan]): Fix[LogicalPlan] = {
    val t1 = t.transCata(repeatedly(simplifyƒ))
    val t2 = t1.boundParaS(rewriteCrossJoinsƒ).evalZero
    val t3 = t2.transCata(repeatedly(simplifyƒ))
    val t4 = (normalizeLets _ >>> normalizeTempNames _)(t3)
    t4
  }
}
