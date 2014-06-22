package slamdata.engine.std

import scalaz._
import Scalaz._

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import SemanticError._
import Validation.{success, failure}
import NonEmptyList.nel

trait StringLib extends Library {
  // TODO: variable arity
  val Concat = Mapping("concat", "Concatenates two (or more) string values", Type.Str :: Type.Str :: Nil,
    ts => ts match {
      case Type.Const(Data.Str(a)) :: Type.Const(Data.Str(b)) :: Nil => success(Type.Const(Data.Str(a + b)))
      
      case Type.Str :: Type.Const(Data.Str(_)) :: Nil => success(Type.Str)
      case Type.Const(Data.Str(_)) :: Type.Str :: Nil => success(Type.Str)
      case Type.Str :: Type.Str :: Nil                => success(Type.Str)
      
      case t :: _ => failure(nel(TypeError(Type.Str, t, None), Nil))
      case Nil    => failure(nel(GenericError("expected arguments for concat"), Nil))
    },
    t => t match {
      case Type.Str => success(Type.Str :: Type.Str :: Nil)
      case _ => failure(nel(TypeError(Type.Str, t, None), Nil))
    }
  )

  def functions = Concat :: Nil
}
object StringLib extends StringLib