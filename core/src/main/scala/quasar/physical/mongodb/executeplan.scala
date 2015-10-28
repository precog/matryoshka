package quasar
package physical
package mongodb

//import quasar.Predef._

//import scalaz._

object executeplan {
  type G[A]            = PhaseResultT[MongoDb, A]
  type MongoExecute[A] = ExecErrT[G, A]

//  val interpret: ExecutePlan ~> MongoExecute = ???
}
