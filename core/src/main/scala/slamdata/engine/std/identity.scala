package slamdata.engine.std

import scalaz._

import slamdata.engine._
import slamdata.engine.fp._

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
      case Type.Const(Data.Str(str)) :: Nil => Type.Const(Data.Id(str))
    },
    Type.typecheck(_, Type.Id) map κ(Type.Str :: Nil)
  )

  val functions = Squash :: ToId :: Nil
}
object IdentityLib extends IdentityLib
