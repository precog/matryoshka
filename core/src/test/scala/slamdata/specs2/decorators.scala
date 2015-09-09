package slamdata.specs2

import slamdata.Predef._

import org.specs2.execute._

/**
 Allows the body of an example to be marked as pending when the specification
 is run normally, but skipped when a special flag is enabled, so that the code
 it purports to test will not be erroneously flagged as covered.
 */
trait PendingWithAccurateCoverage extends PendingUntilFixed {
  def isCoverageRun: Boolean =
    java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isCoverageRun"))

  /** Overrides the standard specs2 implicit. */
  implicit def toPendingWithAccurateCoverage[T: AsResult](t: => T) = new PendingWithAccurateCoverage(t)

  class PendingWithAccurateCoverage[T: AsResult](t: => T) {
    def pendingUntilFixed: Result = pendingUntilFixed("")

    def pendingUntilFixed(m: String): Result =
      if (isCoverageRun) Skipped(m + " (pending example skipped during coverage run)")
      else toPendingUntilFixed(t).pendingUntilFixed(m)
  }
}

object PendingWithAccurateCoverage extends PendingWithAccurateCoverage

trait SkippedDecorator extends PendingUntilFixed {
  implicit def toSkippedDecorator[T: AsResult](t: => T) =
    new SkippedDecorator(t)

  class SkippedDecorator[T: AsResult](t: => T) {
    def skipped: Result = skipped("")

    def skipped(m: String): Result = Skipped(m)
  }
}

object SkippedDecorator extends SkippedDecorator
