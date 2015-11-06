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
  ): Task[FileSystem ~> Task] =
    for {
      rfile  <- readfile.run(client)
      wfile  <- writefile.run(client)
      mfile  <- managefile.run(client, defDb)
    } yield {
      interpretFileSystem(
        rfile compose readfile.interpret,
        wfile compose writefile.interpret,
        mfile compose managefile.interpret)
    }
}
