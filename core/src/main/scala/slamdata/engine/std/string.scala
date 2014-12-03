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
  
  val Like = Mapping(
    "(like)",
    "Determines if a string value matches a pattern.",
    Type.Str :: Type.Str :: Type.Str :: Nil,
    ts => ts match {
      case List(Type.Str, Type.Const(Data.Str(_)), Type.Const(Data.Str(_))) =>
        success(Type.Bool)
      case Type.Str :: _ :: _ :: Nil =>
        failure(nel(GenericError("expected string constant for LIKE"), Nil))
      case t :: Type.Const(Data.Str(_)) :: Nil =>
        failure(nel(TypeError(Type.Str, t, None), Nil))
      case _ =>
        failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Bool) map { _ => List(Type.Str, Type.Str, Type.Str) }
  )

  def matchAnywhere(str: String, pattern: String) = java.util.regex.Pattern.compile(pattern).matcher(str).find()

  val Search = Mapping(
    "search",
    "Determines if a string value matches a regular expresssion.",
    Type.Str :: Type.Str :: Nil,
    ts => ts match {
      case Type.Const(Data.Str(str)) :: Type.Const(Data.Str(pattern)) :: Nil =>
        success(Type.Const(Data.Bool(matchAnywhere(str, pattern))))
      case strT :: patternT :: Nil =>
        (Type.typecheck(strT, Type.Str) |@| Type.typecheck(patternT, Type.Str))((_, _) => Type.Bool)
      case _ =>
        failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Bool) map { _ => Type.Str :: Type.Str :: Nil }
  )

  val Length = Mapping(
    "length",
    "Counts the number of characters in a string.",
    Type.Str :: Nil,
    _ match {
      case Type.Const(Data.Str(str)) :: Nil =>
        success(Type.Const(Data.Int(str.length)))
      case Type.Str :: Nil => success(Type.Int)
      case t :: Nil => failure(nel(TypeError(Type.Str, t, None), Nil))
      case _ => failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Int) map { _ => Type.Str :: Nil }
  )

  val Lower = Mapping(
    "lower",
    "Converts the string to lower case.",
    Type.Str :: Nil,
    _ match {
      case Type.Const(Data.Str(str)) :: Nil =>
        success(Type.Const (Data.Str(str.toLowerCase)))
      case Type.Str :: Nil => success(Type.Str)
      case t :: Nil => failure(nel(TypeError(Type.Str, t, None), Nil))
      case _ => failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Str) map { _ => Type.Str :: Nil }
  )

  val Upper = Mapping(
    "upper",
    "Converts the string to upper case.",
    Type.Str :: Nil,
    _ match {
      case Type.Const(Data.Str(str)) :: Nil =>
        success(Type.Const (Data.Str(str.toUpperCase)))
      case Type.Str :: Nil => success(Type.Str)
      case t :: Nil => failure(nel(TypeError(Type.Str, t, None), Nil))
      case _ => failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Str) map { _ => Type.Str :: Nil }
  )

  val Substring = Mapping(
    "substring",
    "Extracts a portion of the string",
    Type.Str :: Type.Int :: Type.Int :: Nil,
    _ match {
      case Type.Const(Data.Str(str))
          :: Type.Const(Data.Int(from0))
          :: Type.Const(Data.Int(for0))
          :: Nil => {
        val from = from0.intValue - 1
        success(Type.Const(Data.Str(str.substring(from, from + for0.intValue))))
      }
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Const(Data.Int(_)), Type.Int)                =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Const(Data.Int(_))) =>
        success(Type.Str)
      case List(Type.Str, Type.Int,                Type.Int)                =>
        success(Type.Str)
      case Type.Str :: _ :: _ :: Nil =>
        failure(nel(GenericError("expected integer arguments for SUBSTRING"), Nil))
      case t :: _ :: _ :: Nil =>
        failure(nel(TypeError(Type.Str, t, None), Nil))
      case _ =>
        failure(nel(GenericError("expected arguments"), Nil))
    },
    Type.typecheck(_, Type.Str) map { _ => Type.Str :: Type.Int :: Type.Int :: Nil }
  )

  def functions = Concat :: Like :: Search :: Length :: Lower :: Upper :: Substring :: Nil
}
object StringLib extends StringLib
