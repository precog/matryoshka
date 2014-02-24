package slamdata.engine.std

trait StdLib extends Library {
  def math = MathLib

  def structural = StructuralLib

  def agg = AggLib

  def functions = math.functions ++ structural.functions ++ agg.functions ++ Nil
}
object StdLib extends StdLib