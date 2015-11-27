package quasar
package specs2

import org.specs2.mutable.Specification
import org.specs2.specification.{SpecificationStructure, Fragments}

/** Trait that tags all examples in a spec for exclusive execution. Examples
  * will be executed sequentially and parallel execution will be disabled.
  *
  * Use this when you have tests that muck with global state.
  */
trait ExclusiveExecution extends SpecificationStructure { self: Specification =>
  import ExclusiveExecution._

  sequential

  override def map(fs: => Fragments) =
    section(ExclusiveExecutionTag) ^ super.map(fs) ^ section(ExclusiveExecutionTag)
}

object ExclusiveExecution {
  /** The tag that indicates an example should be executed exclusively.
    *
    * NB: Take care when modifying this to update the SBT configuration.
    * TODO: Read this tag name from SBT config
    */
  val ExclusiveExecutionTag = "exclusive"
}
