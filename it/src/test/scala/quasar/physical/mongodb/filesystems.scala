package quasar.physical.mongodb

import quasar.{EnvironmentError2, rethrow}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.mongodb.fs._
import quasar.regression._

import com.mongodb.ConnectionString
import com.mongodb.async.client.MongoClients
import scalaz._
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    cs: ConnectionString,
    prefix: ADir
  ): Task[FileSystem ~> Task] = for {
    client   <- Task.delay(MongoClients create cs)
    mongofs0 <- rethrow[Task, EnvironmentError2]
                  .apply(mongoDbFileSystem(client, DefaultDb fromPath prefix))
    mongofs  =  rethrow[Task, WorkflowExecutionError] compose mongofs0
  } yield mongofs

  def testFileSystemIO(
    cs: ConnectionString,
    prefix: ADir
  ): Task[FileSystemIO ~> Task] =
    testFileSystem(cs, prefix)
      .map(interpret2(NaturalTransformation.refl[Task], _))
}
