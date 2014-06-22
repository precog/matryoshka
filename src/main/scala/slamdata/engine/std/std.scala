package slamdata.engine.std

trait StdLib extends Library {
  val math = MathLib

  val structural = StructuralLib

  val agg = AggLib

  val relations = RelationsLib

  val set = SetLib
  
  val string = StringLib

  val functions = math.functions ++ structural.functions ++ agg.functions ++ relations.functions ++ set.functions ++ string.functions ++ Nil
}
object StdLib extends StdLib