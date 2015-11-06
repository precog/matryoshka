package quasar
package fs

import quasar.Predef._

import pathy.Path._

import scalaz._
import scalaz.concurrent.Task

/** FileSystem Under Test
  *
  * @param name the name of the filesystem
  * @param run an interpreter of the filesystem into the `Task` monad
  * @param prefix a directory in the filesystem tests may use for temp data
  */
final case class FileSystemUT[S[_]](
  name: String,
  run: S ~> Task,
  testDir: AbsDir[Sandboxed]
) {
  def contramap[T[_]](f: T ~> S): FileSystemUT[T] =
    FileSystemUT(name, run compose f, testDir)
}
