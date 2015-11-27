package quasar

import quasar.fs.FileSystem

import scalaz._
import scalaz.concurrent._

package object regression {
  type FileSystemIO[A] = Coproduct[Task, FileSystem, A]
}
