package slamdata.engine

import slamdata.engine.analysis.fixplate._
import slamdata.engine.sql._
import slamdata.engine.SemanticError._

import scalaz._
import Scalaz._

object Variables {
  type VarName = String
  type VarValue = String
  type Variables = Map[VarName, VarValue]

  def coerce(t: Type, varValue: VarValue): Option[Term[Expr]] = {
    def parseExpr(pf: PartialFunction[Term[Expr], Unit]) =
      (new SQLParser()).parseExpr(varValue).toOption.filter(pf.isDefinedAt _)

    t match {
      case Type.Top     => parseExpr { case _ => () }
      case Type.Null    => parseExpr { case Term(NullLiteral) => () }
      case Type.Str |
           Type.Timestamp |
           Type.Date |
           Type.Time |
          Type.Interval =>
        (parseExpr { case Term(StringLiteral(_)) => () })
          .orElse(Some(Term[Expr](StringLiteral(varValue))))
      case Type.Int     => parseExpr { case Term(IntLiteral(_)) => () }
      case Type.Dec     => parseExpr { case Term(FloatLiteral(_)) => () }
      case Type.Bool    => parseExpr { case Term(BoolLiteral(_)) => () }

      case _ => None
    }
  }

  def substVars[A](tree: Cofree[Expr, A], typeProj: A => Type, vars: Variables):
      Error \/ Cofree[Expr, A] =
    (tree.tail match {
      case v @ Vari(name) if vars.contains(name) =>
        coerce(typeProj(tree.head), vars(name)) \/>
          VariableTypeError(name, tpe, varValue)
      case x => \/-(Term(x))
    }).map(Cofree(_, tree.head))
}
