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

/** Implementation class for a WorkflowExecutor in the `MongoDb` monad. */
private[mongodb] final class MongoDbWorkflowExecutor
  extends WorkflowExecutor[MongoDb] {

  def aggregate(src: Collection, pipeline: Pipeline) =
    MongoDb.aggregate_(src, pipeline map (_.bson.repr), true)

  def drop(c: Collection) =
    MongoDb.dropCollection(c)

  def insert(dst: Collection, values: List[Bson.Doc]) =
    MongoDb.insert(dst, values map (_.repr))

  def mapReduce(src: Collection, dstCollectionName: String, mr: MapReduce) = {
    val lim =
      mr.limit.cata(
        l => Positive(l).cata(
          _.some.point[MongoDb],
          MongoDb.fail(new IllegalArgumentException(
            s"limit must be a positive number: $l"))),
        none[Positive].point[MongoDb])

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
        ).point[MongoDb]

      case Some(MapReduce.Inline) =>
        MongoDb.fail(new IllegalArgumentException(
          s"Destination collection, `$dstCollectionName`, given but MapReduce specified inline output."))

      case None =>
        MapReduce.OutputCollection(dstCollectionName, None).point[MongoDb]
    }

    (outColl |@| cfg)(MongoDb.mapReduce_(src, _, _)).join
  }

  def rename(src: Collection, dst: Collection) =
    MongoDb.rename(src, dst, RenameSemantics.Overwrite)
}

private[mongodb] object MongoDbWorkflowExecutor {

  /** The minimum MongoDb version required to be able to execute `Workflow`s. */
  val MinMongoDbVersion = List(2, 6, 0)

  /** Catch MongoExceptions and attempt to convert to EnvironmentError2. */
  val liftEnvErr: MongoDb ~> EnvErr2T[MongoDb, ?] =
    new (MongoDb ~> EnvErr2T[MongoDb, ?]) {
      def apply[A](m: MongoDb[A]) = EitherT(m.attemptMongo.run flatMap {
        case -\/(ex: MongoSocketOpenException) =>
          ConnectionFailed2(ex.getMessage).left.point[MongoDb]

        case -\/(ex: MongoSocketException) =>
          ConnectionFailed2(ex.getMessage).left.point[MongoDb]

        case -\/(ex) if ex.getMessage contains "Command failed with error 18: 'auth failed'" =>
          InvalidCredentials2(ex.getMessage).left.point[MongoDb]

        case -\/(ex) =>
          MongoDb.fail(ex)

        case \/-(a) =>
          a.right.point[MongoDb]
      })
    }
}
