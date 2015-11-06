package quasar

import scalaz._
import scalaz.concurrent._

package object regression {
  type QueryFsIO[A] = Coproduct[Task, QueryableFileSystem, A]
}
