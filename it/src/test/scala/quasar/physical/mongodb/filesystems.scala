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
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    cs: ConnectionString,
    prefix: AbsDir[Sandboxed]
  ): Task[FileSystem ~> Task] = for {
    defDb    <- defaultDb(cs, prefix)
    client   <- Task.delay(MongoClients create cs)
    mongofs0 <- rethrow[Task, EnvironmentError2]
                  .apply(mongoDbFileSystem(client, defDb))
    mongofs  =  rethrow[Task, WorkflowExecutionError] compose mongofs0
  } yield mongofs

  def testFileSystemIO(
    cs: ConnectionString,
    prefix: AbsDir[Sandboxed]
  ): Task[FileSystemIO ~> Task] =
    testFileSystem(cs, prefix)
      .map(interpret.interpret2(NaturalTransformation.refl[Task], _))

  ////

  private def defaultDb(
    cs: ConnectionString,
    prefix: AbsDir[Sandboxed]
  ): Task[DefaultDb] = {
    def noDefaultDbError = new RuntimeException(
      s"Unable to determine a default database for `${cs.toString}` from `${posixCodec.printPath(prefix)}`."
    )

    DefaultDb fromPath prefix cata (_.point[Task], Task.fail(noDefaultDbError))
  }
}
