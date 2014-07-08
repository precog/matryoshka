package test
import slamdata.engine.BackendDefinitions._

object Test4 extends App {
  val q = "select city, pop from zips where pop between 10 and 15"
  
  println(MongoDB.eval(Query(q), Path("tmp")))
}