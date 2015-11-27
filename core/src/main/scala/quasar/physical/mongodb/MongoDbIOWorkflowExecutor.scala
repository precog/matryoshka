package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fs.Positive
import quasar.physical.mongodb.workflowtask._

import java.lang.IllegalArgumentException

import com.mongodb._
import org.bson.Document

import scalaz._, Scalaz._

/** Implementation class for a WorkflowExecutor in the `MongoDbIO` monad. */
private[mongodb] final class MongoDbWorkflowExecutor
  extends WorkflowExecutor[MongoDbIO] {

  import MapReduce._

  def aggregate(src: Collection, pipeline: Pipeline) =
    MongoDbIO.aggregate_(src, pipeline map (_.bson.repr), true)

  def drop(c: Collection) =
    MongoDbIO.dropCollection(c)

  def insert(dst: Collection, values: List[Bson.Doc]) =
    MongoDbIO.insert(dst, values map (_.repr))

  def mapReduce(src: Collection, dst: OutputCollection, mr: MapReduce) =
    MongoDbIO.mapReduce_(src, dst, mr)

  def rename(src: Collection, dst: Collection) =
    MongoDbIO.rename(src, dst, RenameSemantics.Overwrite)
}

private[mongodb] object MongoDbWorkflowExecutor {
  import EnvironmentError2._

  /** The minimum MongoDbIO version required to be able to execute `Workflow`s. */
  val MinMongoDbVersion = List(2, 6, 0)

  /** Catch MongoExceptions and attempt to convert to EnvironmentError2. */
  val liftEnvErr: MongoDbIO ~> EnvErr2T[MongoDbIO, ?] =
    new (MongoDbIO ~> EnvErr2T[MongoDbIO, ?]) {
      def apply[A](m: MongoDbIO[A]) = EitherT(m.attemptMongo.run flatMap {
        case -\/(ex: MongoSocketOpenException) =>
          ConnectionFailed(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex: MongoSocketException) =>
          ConnectionFailed(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex) if ex.getMessage contains "Command failed with error 18: 'auth failed'" =>
          InvalidCredentials(ex.getMessage).left.point[MongoDbIO]

        case -\/(ex) =>
          MongoDbIO.fail(ex)

        case \/-(a) =>
          a.right.point[MongoDbIO]
      })
    }
}
