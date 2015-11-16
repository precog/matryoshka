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
import quasar.recursionschemes._, Recursive.ops._, FunctorT.ops._

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

  val namesƒ: LogicalPlan[Set[Symbol]] => Set[Symbol] = {
    case FreeF(name) => Set(name)
    case x           => x.fold
  }

  def freshName[F[_]: Functor: Foldable](
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

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      LogicalPlan[(
        Fix[LogicalPlan],
        (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]))] =>
  (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]) = { node =>
    def preserveFree(x: (Fix[LogicalPlan], (Fix[LogicalPlan], Option[List[Fix[LogicalPlan]]]))):
        Fix[LogicalPlan] = x._1.unFix match {
      case FreeF(_) => x._1
      case _        => x._2._1
    }

    (node match {
      case InvokeF(DeleteField, List(src, field)) =>
        src._2._2.fold(
          Invoke(DeleteField, List(preserveFree(src), preserveFree(field)))) {
          fields =>
            val name = freshName("src", fields)
              Let(name, preserveFree(src),
                Fix(MakeObjectN(fields.filterNot(_ == field._2._1).map(f =>
                  f -> Invoke(ObjectProject, List(Free(name), f))): _*)))
        }
      case lp => Fix(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Fix[LogicalPlan]): Fix[LogicalPlan] =
    boundPara(t)(preferProjectionsƒ)._1.transCata(repeatedly(simplifyƒ))

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

}
