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

  def aggregate(src: Collection, pipeline: Pipeline) =
    MongoDbIO.aggregate_(src, pipeline map (_.bson.repr), true)

  def drop(c: Collection) =
    MongoDbIO.dropCollection(c)

  def insert(dst: Collection, values: List[Bson.Doc]) =
    MongoDbIO.insert(dst, values map (_.repr))

  def mapReduce(src: Collection, dstCollectionName: String, mr: MapReduce) = {
    val lim =
      mr.limit.cata(
        l => Positive(l).cata(
          _.some.point[MongoDbIO],
          MongoDbIO.fail(new IllegalArgumentException(
            s"limit must be a positive number: $l"))),
        none[Positive].point[MongoDbIO])

    val maybeScope =
      if (mr.scope.isEmpty)
        none[Document]
      else
        Bson.Doc(mr.scope).repr.some

    val maybeSort =
      mr.inputSort map (ts =>
        Bson.Doc(ListMap(ts.list.map(_.bimap(_.asText, _.bson)): _*))
          .repr)

    val cfg = lim map (l =>
      MapReduce.Config(
        finalizer = mr.finalizer.map(_.pprint(0)),
        inputFilter = mr.selection.map(_.bson.repr),
        inputLimit = l,
        map = mr.map.pprint(0),
        reduce = mr.reduce.pprint(0),
        scope = maybeScope,
        sort = maybeSort,
        useJsMode = mr.jsMode.getOrElse(false),
        verboseResults = mr.verbose.getOrElse(true)))

    val outColl = mr.out match {
      case Some(MapReduce.WithAction(act, db, sharded)) =>
        MapReduce.OutputCollection(
          dstCollectionName,
          Some(MapReduce.ActionedOutput(act, db, sharded))
        ).point[MongoDbIO]

      case Some(MapReduce.Inline) =>
        MongoDbIO.fail(new IllegalArgumentException(
          s"Destination collection, `$dstCollectionName`, given but MapReduce specified inline output."))

      case None =>
        MapReduce.OutputCollection(dstCollectionName, None).point[MongoDbIO]
    }

    (outColl |@| cfg)(MongoDbIO.mapReduce_(src, _, _)).join
  }

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
