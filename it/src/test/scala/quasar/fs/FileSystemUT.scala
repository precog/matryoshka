package quasar
package fs

import scalaz._
import scalaz.concurrent.Task

/** FileSystem Under Test
  *
  * @param name the name of the filesystem
  * @param run an interpreter of the filesystem into the `Task` monad
  * @param prefix a directory in the filesystem tests may use for temp data
  */
final case class FileSystemUT[S[_]](
  name: BackendName,
  run: S ~> Task,
  testDir: ADir
) {
  def contramap[T[_]](f: T ~> S): FileSystemUT[T] =
    FileSystemUT(name, run compose f, testDir)
}
