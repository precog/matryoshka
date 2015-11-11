package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.regression._
import quasar.fp.interpret
import quasar.fs._
import quasar.physical.mongodb.fs._

import com.mongodb.ConnectionString
import com.mongodb.async.client.{MongoClient, MongoClients}

import pathy.Path._

import scalaz._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    cs: ConnectionString,
    prefix: AbsDir[Sandboxed]
  ): Task[FileSystem ~> Task] =
    Task.delay(MongoClients create cs) flatMap (testFileSystem0(_, prefix))

  def testQueryableFileSystem(
    cs: ConnectionString,
    prefix: AbsDir[Sandboxed]
  ): Task[QueryFsIO ~> Task] = for {
    client   <- Task.delay(MongoClients create cs)
    mongofs  <- testFileSystem0(client, prefix)
    mongoex0 <- rethrow[Task, EnvironmentError2].apply(executeplan.run(client))
    mongoex  =  rethrow[Task, WorkflowExecutionError] compose mongoex0
  } yield interpret.interpret3(NaturalTransformation.refl[Task], mongoex, mongofs)

  ////

  private def testFileSystem0(
    client: MongoClient,
    prefix: AbsDir[Sandboxed]
  ): Task[FileSystem ~> Task] = {
    def noDefaultDbError = Task.fail(new RuntimeException(
      s"Unable to determine a default database for `${client.toString}` from `${posixCodec.printPath(prefix)}`."
    ))

    DefaultDb.fromPath(prefix)
      .cata(mongoDbFileSystem(client, _), noDefaultDbError)
  }
}
