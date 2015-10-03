package quasar
package physical
package mongodb

import quasar.Predef._

package object fs {
  final case class DefaultDb(run: String) extends AnyVal
  final case class TmpPrefix(run: String) extends AnyVal
}
