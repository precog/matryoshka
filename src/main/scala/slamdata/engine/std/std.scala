package slamdata.engine.std

trait StdLib extends Library {
  def math = MathLib

  def structural = StructuralLib

  def functions = math.functions ++ structural.functions ++ Nil
}
object StdLib extends StdLib