package slamdata.engine.std

import slamdata.engine.{Func, Type, MappingType}

trait MathLib extends Library {
  private def binOp(name: String, help: String) = 
    Func(name, help, (Type.Dec | Type.Int) :: Nil, Type.Dec, MappingType.OneToOne, Some(ds => ds.head))

  def functions = 
    binOp("(+)",  "Adds two numeric values") ::
    binOp("(*)",  "Multiplies two numeric values") ::
    binOp("(-)",  "Subtracts two numeric values") ::
    binOp("(/)",  "Divides one numeric value by another (non-zero) numeric value") ::
    Nil
}
object MathLib extends MathLib