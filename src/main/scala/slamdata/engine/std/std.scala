package slamdata.engine.std

trait StdLib extends Library {
  def functions = MathLib.functions ++ Nil
}
object StdLib extends StdLib