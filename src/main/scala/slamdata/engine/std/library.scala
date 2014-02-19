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
  
  protected implicit class RefinderW(self: Func.CodomainRefiner) {
    def ||| (that: Func.CodomainRefiner): Func.CodomainRefiner = {
      args => self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}