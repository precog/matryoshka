package slamdata.engine.std

import scalaz._

import scalaz.std.list._

import slamdata.engine.{Func, Type, SemanticError, Data}

trait Library {
  protected val typeOf = (v: Type \/ Data) => v.fold(identity, _.dataType)

  protected def fixedRefiner(types: List[Type], codomain: Type): Func.CodomainRefiner = {
    val typesi = types.zipWithIndex

    args => 
      Traverse[List].sequence[({type f[A]=ValidationNel[SemanticError, A]})#f, Type](typesi.zip((args.map {
        case -\/(typ) => typ
        case \/-(data) => data.dataType
      })).map {
        case ((expected, index), actual) => expected.unify(actual)
      }).map(Function.const(-\/(codomain)))
  }

  protected def constRefiner(codomain: Type): Func.CodomainRefiner = {
    args => Validation.success(-\/(codomain))
  }

  protected def wideningRefiner(o: Order[Type]): Func.CodomainRefiner = {
    args => Validation.success(-\/(args.map(typeOf).sortWith((a, b) => o.order(a, b) == Ordering.LT).head))
  }

  protected def partialRefiner(f: PartialFunction[List[Type \/ Data], Type \/ Data]): Func.CodomainRefiner = {
    args =>
      f.lift(args).map(Validation.success).getOrElse(
        Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
      )
  }

  protected def partialRefinerV(f: PartialFunction[List[Type \/ Data], ValidationNel[SemanticError, Type \/ Data]]): Func.CodomainRefiner = {
    args =>
      f.lift(args).getOrElse(
        Validation.failure(NonEmptyList(SemanticError.GenericError("Unknown arguments: " + args)))
      )
  }

  protected def dataRefiner(func: List[Data] => Data): Func.CodomainRefiner = {
    args =>
      (args.foldLeft[Option[Vector[Data]]](Some(Vector.empty[Data])) {
        case (None, _) => None
        case (Some(vector), -\/(typ)) => None
        case (Some(vector), \/-(data)) => Some(vector :+ data)
      }).map(args => Validation.success(\/-(func(args.toList)))).getOrElse {
        Validation.failure(NonEmptyList(SemanticError.GenericError("Expected all arguments to be constant data")))
      }
  }

  protected implicit class RefinderW(self: Func.CodomainRefiner) {
    def ||| (that: Func.CodomainRefiner): Func.CodomainRefiner = {
      args => self(args) ||| that(args)
    }
  }

  def functions: List[Func]
}