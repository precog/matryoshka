package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.sql._
import slamdata.engine.SemanticError._

import argonaut._, Argonaut._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._

case class Variables(value: Map[VarName, VarValue])
case class VarName(value: String) {
  override def toString = ":" + value
}
case class VarValue(value: String)

object Variables {
  def fromMap(value: Map[String, String]): Variables = Variables(value.map(t => VarName(t._1) -> VarValue(t._2)))

  def coerce(t: Type, varValue: VarValue): Option[Expr] = {     
    def parseExpr(pf: PartialFunction[Expr, Unit]) = 
      (new SQLParser()).parseExpr(varValue.value).toOption.filter(pf.isDefinedAt _)

    t match {
      case Type.Top       => parseExpr { case _ => () }
      case Type.Null      => parseExpr { case NullLiteral() => () }
      case Type.Str      | 
           Type.DateTime | 
           Type.Interval  => (parseExpr { case StringLiteral(_) => () }).orElse(Some(StringLiteral(varValue.value)))
      case Type.Int       => parseExpr { case IntLiteral(_) => () }
      case Type.Dec       => parseExpr { case FloatLiteral(_) => () }
      case Type.Bool      => parseExpr { case BoolLiteral(_) => () }

      case _ => None
    }
  }

  def substVars[A](tree: AnnotatedTree[Node, A], typeProj: A => Type, vars: Variables): Error \/ AnnotatedTree[Node, A] = {
    type S = List[(Node, A)]
    type EitherM[A] = EitherT[Free.Trampoline, Error, A]
    type M[A] = StateT[EitherM, S, A]

    def typeOf(n: Node) = typeProj(tree.attr(n))

    def unchanged[A <: Node](t: (A, A)): M[A] = changed(t._1, \/- (t._2))

    def changed[A <: Node](old: A, new0: Error \/ A): M[A] = StateT[EitherM, S, A] { state =>
      EitherT(new0.map { new0 =>
        val ann = tree.attr(old)

        (((new0 -> ann) :: state, new0))
      }.point[Free.Trampoline])
    }

    tree.root.mapUpM0[M](
      unchanged _,
      unchanged _,
      unchanged _,
      {
        case (old, v @ Vari(name)) if vars.value.contains(VarName(name)) => 
          val tpe  = typeOf(v)
          val varValue = vars.value(VarName(name))

          lazy val error: Error = VariableTypeError(VarName(name), tpe, varValue)
      
          changed(old, coerce(tpe, varValue) \/> (error))

        case t => unchanged(t)
      },
      unchanged _,
      unchanged _,
      unchanged _
    ).run(Nil).run.run.map { 
      case (tuples, root) => 
        val map1 = tuples.foldLeft(new java.util.IdentityHashMap[Node, A]) { // TODO: Use ordinary map when AnnotatedTree has been off'd
          case (map, (k, v)) => map.put(k, v); map
        }

        Tree[Node](root, _.children).annotate(map1.get(_))
    }
  }
}
