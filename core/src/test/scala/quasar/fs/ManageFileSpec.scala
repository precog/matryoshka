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

  "ManageFile" should {
    "renameFile" >> {
      "moves the existing file to a new name in the same directory" ! prop {
        (s: SingleFileMemState, name: String) => {
          val rename =
            manage.renameFile(s.file, name).liftM[Process]
          val existsP: Process[manage.M, Boolean] =
            query.fileExists(s.file).liftM[Process]
          val existsAndData: Process[manage.M, (Boolean, Data)] =
            existsP tuple read.scanAll(fileParent(s.file) </> file(name))

          MemTask.runLogT(rename.drain ++ existsAndData)
            .map(_.unzip.leftMap(_ exists ι))
            .run.eval(s.state)
            .run.toEither must beRight((false, s.contents))
        }
      }
    }
  }
}
