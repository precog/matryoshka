package slamdata.engine.std

import slamdata.engine.{Data, Func, Type, Mapping, SemanticError}

import scalaz._

trait ObjectLib extends Library {
  val ToId = Mapping(
    "oid",
    "Converts a string literal to a (backend-specific) object identifier.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.Id
    },
    Type.typecheck(_, Type.Id) map { _ => Type.Str :: Nil }
  )

  val functions = ToId :: Nil
}
object ObjectLib extends ObjectLib