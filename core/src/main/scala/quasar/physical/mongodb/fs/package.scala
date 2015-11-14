package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => _, _}

import com.mongodb.async.client.MongoClient

import pathy._, Path._

import scalaz.~>
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.monadPlus._
import scalaz.concurrent.Task

package object fs {
  type WFTask[A] = WorkflowExecErrT[Task, A]

  final case class DefaultDb(run: String) extends AnyVal

  object DefaultDb {
    def fromPath[T](path: Path[Abs, T, Sandboxed]): Option[DefaultDb] =
      flatten(none, none, none, _.some, κ(none), path)
        .unite.headOption map (DefaultDb(_))
  }

  final case class TmpPrefix(run: String) extends AnyVal

  def mongoDbFileSystem(
    client: MongoClient,
    defDb: DefaultDb
  ): EnvErr2T[Task, FileSystem ~> WFTask] =
    for {
      qfile  <- queryfile.run(client)(queryfile.interpret)
      rfile  <- readfile.run(client).liftM[EnvErr2T]
      wfile  <- writefile.run(client).liftM[EnvErr2T]
      mfile  <- managefile.run(client, defDb).liftM[EnvErr2T]
      liftWF =  liftMT[Task, WorkflowExecErrT]
    } yield {
      interpretFileSystem[WFTask](
        qfile,
        liftWF compose rfile compose readfile.interpret,
        liftWF compose wfile compose writefile.interpret,
        liftWF compose mfile compose managefile.interpret)
    }
}
