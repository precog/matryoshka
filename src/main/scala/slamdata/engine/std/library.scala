package slamdata.engine.std

import scalaz._

import scalaz.std.list._

import slamdata.engine.{Func, Type, SemanticError, Data}

trait Library {
  protected def constRefiner(codomain: Type): Func.CodomainRefiner = {
    args => Validation.success(codomain)
  }

  protected def wideningRefiner(o: Order[Type]): Func.CodomainRefiner = {
    args => Validation.success(args.sortWith((a, b) => o.order(a, b) == Ordering.LT).head)
  }

  protected def partialRefiner(f: PartialFunction[List[Type], Type]): Func.CodomainRefiner = {
    args =>
      f.lift(args).map(Validation.success).getOrElse(
        Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
      )
  }

  protected def partialRefinerV(f: PartialFunction[List[Type], ValidationNel[SemanticError, Type]]): Func.CodomainRefiner = {
    args =>
      f.lift(args).getOrElse(
        Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
      )
  }

  protected val numericWidening: Func.CodomainRefiner = wideningRefiner(new Order[Type] {
    def order(v1: Type, v2: Type) = (v1, v2) match {
      case (Type.Dec, Type.Dec) => Ordering.EQ
      case (Type.Dec, _) => Ordering.LT
      case (_, Type.Dec) => Ordering.GT
      case _ => Ordering.EQ
    }
  })

  protected implicit class RefinderW(self: Func.CodomainRefiner) {
    def ||| (that: Func.CodomainRefiner): Func.CodomainRefiner = {
      args => self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}