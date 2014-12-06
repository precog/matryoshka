package slamdata.engine.std

import scalaz._

import scalaz.std.list._

import slamdata.engine.{Func, Type, SemanticError, Data}

import Validation.{success, failure}

trait Library {
  protected def constTyper(codomain: Type): Func.Typer = { args => 
    Validation.success(codomain)
  }

  protected def wideningTyper(o: Order[Type]): Func.Typer = { args => 
    args.sortWith((a, b) => o.order(a, b) == Ordering.LT)
        .headOption
        .fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(SemanticError.GenericError("No arguments"))))(
          success(_))
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

  protected val numericWidening: Func.Typer = wideningTyper(new Order[Type] {
    def order(v1: Type, v2: Type) = (v1, v2) match {
      case (Type.Dec, Type.Dec) => Ordering.EQ
      case (Type.Dec, _) => Ordering.LT
      case (_, Type.Dec) => Ordering.GT
      case _ => Ordering.EQ
    }
  })

  protected implicit class TyperW(self: Func.Typer) {
    def ||| (that: Func.Typer): Func.Typer = { args => 
      self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}
