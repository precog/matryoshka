package quasar
package fs

import quasar.Predef._
import quasar.fp._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.stream._

class ManageFileSpec extends Specification with ScalaCheck with FileSystemFixture {
  import inmemory._, DataGen._, PathyGen._

  "ManageFile" should {
    "renameFile" >> {
      "moves the existing file to a new name in the same directory" ! prop {
        (f: AbsFile[Sandboxed], xs: Vector[Data], name: String) => (xs.nonEmpty) ==> {
          // TODO: switch to fileParent once Pathy is updated
          val parent = parentDir(f).get

          val rename =
            manage.renameFile(f, name).liftM[Process]
          val existsP: Process[manage.M, Boolean] =
            query.fileExists(f).liftM[FileSystemErrT].liftM[Process]
          val existsAndData: Process[manage.M, (Boolean, Data)] =
            existsP tuple read.scanAll(parent </> file(name))

          runLog(rename.drain ++ existsAndData)
            .map(_.unzip.leftMap(_ exists ι))
            .run.eval(InMemState fromFiles Map(f -> xs))
            .run.toEither must beRight((false, xs))
        }
      }
    }
  }
}
