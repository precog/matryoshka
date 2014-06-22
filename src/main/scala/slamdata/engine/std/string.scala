package slamdata.engine.std

import scalaz._
import Scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait StringLib extends Library {
  private def stringApply(f: (String, String) => String): Func.Typer = {
      case Type.Const(Data.Str(a)) :: Type.Const(Data.Str(b)) :: Nil => success(Type.Const(Data.Str(f(a, b))))
      
      case Type.Str :: Type.Const(Data.Str(_)) :: Nil => success(Type.Str)
      case Type.Const(Data.Str(_)) :: Type.Str :: Nil => success(Type.Str)
      case Type.Str :: Type.Str :: Nil                => success(Type.Str)
      
      case t :: _ => failure(nel(TypeError(Type.Str, t, None), Nil))
      case Nil    => failure(nel(GenericError("expected arguments"), Nil))
    }
  
  private val StringUnapply: Func.Untyper = {
    case Type.Str => success(Type.Str :: Type.Str :: Nil)
    case t => failure(nel(TypeError(Type.Str, t, None), Nil))
  }

  // TODO: variable arity
  val Concat = Mapping("concat", "Concatenates two (or more) string values", Type.Str :: Type.Str :: Nil,
    stringApply(_ + _),
    StringUnapply
  )
  
  val Like = Mapping("(like)", "Determines if a string value matches a pattern", Type.Str :: Type.Str :: Nil,
    ts => ts match {
      case Type.Str :: Type.Const(Data.Str(_)) :: Nil => success(Type.Bool)
      
      case Type.Str :: t :: Nil                => failure(nel(GenericError("expected string constant for LIKE"), Nil))
      case t :: Type.Const(Data.Str(_)) :: Nil => failure(nel(TypeError(Type.Str, t, None), Nil))
      case _                                   => failure(nel(GenericError("expected arguments"), Nil))
      },
    t => t match {
      case Type.Bool => success(Type.Str :: Type.Str :: Nil)
      case _ => failure(nel(TypeError(Type.Numeric, t, Some("boolean function where non-boolean expression is expected")), Nil))
    }
  )

  def functions = Concat :: Like :: Nil
}
object StringLib extends StringLib