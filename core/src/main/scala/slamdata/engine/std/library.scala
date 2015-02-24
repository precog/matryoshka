package slamdata.engine.std

import scalaz._

import scalaz.std.list._

import slamdata.engine.{Func, Type, SemanticError, Data}

import Validation.{success, failure}

trait Library {
  protected def constTyper(codomain: Type): Func.Typer = { args =>
    Validation.success(codomain)
  }

  protected def partialTyper(f: PartialFunction[List[Type], Type]): Func.Typer = { args =>
    f.lift(args).map(Validation.success).getOrElse(
      Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
    )
  }

  protected def reflexiveTyper: Func.Typer = {
    case Type.Const(data) :: Nil => success(data.dataType)
    case x :: Nil => success(x)
    case _ => failure(NonEmptyList(SemanticError.GenericError("Wrong number of arguments for reflexive typer")))
  }

  protected def partialTyperV(f: PartialFunction[List[Type], ValidationNel[SemanticError, Type]]): Func.Typer = { args =>
    f.lift(args).getOrElse(
      Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
    )
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
