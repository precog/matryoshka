package slamdata.engine

import slamdata.engine.analysis.fixplate._

import scalaz._
import Scalaz._

object Optimizer {
  import LogicalPlan._
  import slamdata.engine.std.StdLib._
  import structural._

  private def countUsage(target: Symbol): LogicalPlan[Int] => Int = {
    case FreeF(symbol) if symbol == target => 1
    case LetF(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inline[A](target: Symbol, repl: Term[LogicalPlan]):
      LogicalPlan[(Term[LogicalPlan], Term[LogicalPlan])] => Term[LogicalPlan] =
    {
      case FreeF(symbol) if symbol == target => repl
      case LetF(ident, form, body) if ident == target =>
        Let(ident, form._2, body._1)
      case x => Term(x.map(_._2))
    }

  val simplify: LogicalPlan[Term[LogicalPlan]] => Term[LogicalPlan] = {
    case InvokeF(func, args) => func.simplify(args)
    case JoinF(Term(ConstantF(Data.Set(Nil))), Term(ConstantF(Data.Set(Nil))), _, _, _, _) => Constant(Data.Set(Nil))
    case JoinF(Term(ConstantF(Data.Set(Nil))), _, JoinType.Inner | JoinType.LeftOuter, _, _, _) => Constant(Data.Set(Nil))
    case JoinF(_, Term(ConstantF(Data.Set(Nil))), JoinType.Inner | JoinType.RightOuter, _, _, _) => Constant(Data.Set(Nil))
    case LetF(ident, form @ Term(ConstantF(_)), in) =>
      in.para(inline(ident, form))
    case LetF(ident, form, in) => in.cata(countUsage(ident)) match {
      case 0 => in
      case 1 => in.para(inline(ident, form))
      case _ => Let(ident, form, in)
    }
    case x => Term(x)
  }

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      LogicalPlan[(
        Term[LogicalPlan],
        (Term[LogicalPlan], Option[List[Term[LogicalPlan]]]))] =>
  (Term[LogicalPlan], Option[List[Term[LogicalPlan]]]) = { node =>
    def preserveFree(x: (Term[LogicalPlan], (Term[LogicalPlan], Option[List[Term[LogicalPlan]]]))):
        Term[LogicalPlan] = x._1.unFix match {
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
      case lp => Term(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Term[LogicalPlan]): Term [LogicalPlan] =
    boundPara(t)(preferProjectionsƒ)._1.cata(simplify)
}
