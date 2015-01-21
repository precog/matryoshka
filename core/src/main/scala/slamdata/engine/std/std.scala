package slamdata.engine.std

trait StdLib extends Library {
  val math = MathLib

  val structural = StructuralLib

  val agg = AggLib

  val identity = IdentityLib

  val relations = RelationsLib

  val set = SetLib

  val array = ArrayLib

  val string = StringLib

  val date = DateLib

  val functions = math.functions ++ structural.functions ++ agg.functions ++ identity.functions ++ relations.functions ++ set.functions ++ array.functions ++ string.functions ++ date.functions ++ Nil
}
object StdLib extends StdLib
