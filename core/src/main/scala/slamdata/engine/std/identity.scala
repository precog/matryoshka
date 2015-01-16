package slamdata.engine.std

import scalaz._

import slamdata.engine._

trait IdentityLib extends Library {
  import Type._
  import Validation.{success, failure}

  val Squash = Squashing("SQUASH", "Squashes all dimensional information", Top :: Nil, partialTyper {
    case x :: Nil => x
  }, {
    case tpe => success(tpe :: Nil)
  })

  val ToId = Mapping(
    "oid",
    "Converts a string literal to a (backend-specific) object identifier.",
    Type.Str :: Nil,
    partialTyper {
      case Type.Const(Data.Str(_)) :: Nil => Type.Id
    },
    Type.typecheck(_, Type.Id) map { _ => Type.Str :: Nil }
  )

  val functions = Squash :: ToId :: Nil
}
object IdentityLib extends IdentityLib