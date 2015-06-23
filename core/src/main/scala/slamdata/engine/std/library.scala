package slamdata.engine.std

import scalaz._

import slamdata.engine.{Func, LogicalPlan, Type, SemanticError}
import slamdata.engine.analysis.fixplate._

import Validation.{success, failure}

trait Library {
  protected val noSimplification: Func.Simplifier =
    args => LogicalPlan.Invoke(_, args)

  protected def partialSimplifier(
    f: PartialFunction[List[Term[LogicalPlan]], Term[LogicalPlan]]):
      Func.Simplifier =
    args => func => f.lift(args).getOrElse(LogicalPlan.Invoke(func, args))

  protected def constTyper(codomain: Type): Func.Typer = { args =>
    Validation.success(codomain)
  }

  private def partialTyperOV(f: List[Type] => Option[ValidationNel[SemanticError, Type]]):
      Func.Typer = {
    args =>
    f(args).getOrElse(Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args))))
  }

  protected def partialTyperV(f: PartialFunction[List[Type], ValidationNel[SemanticError, Type]]):
      Func.Typer =
    partialTyperOV(f.lift)

  protected def partialTyper(f: PartialFunction[List[Type], Type]): Func.Typer =
    partialTyperOV(f.lift(_).map(success))

  private def partialUntyperOV(codomain: Type)(f: Type => Option[ValidationNel[SemanticError, List[Type]]]):
      Func.Untyper = rez => {
    f(rez).getOrElse(failure(NonEmptyList(SemanticError.TypeError(codomain, rez, None))))
  }

  protected def partialUntyperV(
    codomain: Type)(
    f: PartialFunction[Type, ValidationNel[SemanticError, List[Type]]]):
      Func.Untyper =
    partialUntyperOV(codomain)(f.lift)

  protected def partialUntyper(
    codomain: Type)(
    f: PartialFunction[Type, List[Type]]):
      Func.Untyper =
    partialUntyperOV(codomain)(f.lift(_).map(success))

  protected def reflexiveTyper: Func.Typer = {
    case Type.Const(data) :: Nil => success(data.dataType)
    case x :: Nil => success(x)
    case _ => failure(NonEmptyList(SemanticError.GenericError("Wrong number of arguments for reflexive typer")))
  }

  protected val numericWidening = {
    def mapFirst[A, B](f: A => A, p: PartialFunction[A, B]) = new PartialFunction[A, B] {
      def isDefinedAt(a: A) = p.isDefinedAt(f(a))
      def apply(a: A) = p(f(a))
    }

    val half: PartialFunction[List[Type], Type] = {
      case t1 :: t2 :: Nil       if t1 contains t2       => t1
      case Type.Dec :: t2 :: Nil if Type.Int contains t2 => Type.Dec
      case Type.Int :: t2 :: Nil if Type.Dec contains t2 => Type.Dec
    }
    partialTyper(half orElse mapFirst[List[Type], Type](_.reverse, half))
  }

  protected implicit class TyperW(self: Func.Typer) {
    def ||| (that: Func.Typer): Func.Typer = { args =>
      self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}
