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

import quasar.recursionschemes._, Recursive.ops._

import scalaz._
import Scalaz._

object Optimizer {
  import LogicalPlan._
  import quasar.std.StdLib._
  import structural._

  private def countUsage(target: Symbol): LogicalPlan[Int] => Int = {
    case FreeF(symbol) if symbol == target => 1
    case LetF(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inline[A](target: Symbol, repl: Fix[LogicalPlan]):
      LogicalPlan[(Fix[LogicalPlan], Fix[LogicalPlan])] => Fix[LogicalPlan] =
    {
      case FreeF(symbol) if symbol == target => repl
      case LetF(ident, form, body) if ident == target =>
        Let(ident, form._2, body._1)
      case x => Fix(x.map(_._2))
    }

  val simplify: LogicalPlan[Fix[LogicalPlan]] => Fix[LogicalPlan] = {
    case v @ InvokeF(func, args) =>
      func.simplify(args).fold(Fix(v))(x => simplify(x.unFix))
    case LetF(ident, form @ Fix(ConstantF(_)), in) =>
      in.para(inline(ident, form))
    case LetF(ident, form, in) => in.cata(countUsage(ident)) match {
      case 0 => in
      case 1 => in.para(inline(ident, form))
      case _ => Let(ident, form, in)
    }
    case x => Fix(x)
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
                MakeObjectN(fields.filterNot(_ == field._2._1).map(f =>
                  f -> Invoke(ObjectProject, List(Free(name), f))): _*))}
      case lp => Fix(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Fix[LogicalPlan]): Fix [LogicalPlan] =
    boundPara(t)(preferProjectionsƒ)._1.cata(simplify)
}
