package slamdata.engine.physical.mongodb

import slamdata.engine._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.analysis._
import slamdata.engine.sql.SQLParser
import slamdata.engine.std._

import scalaz._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, Expectable}

class PlannerSpec extends Specification {
  import StdLib._
  import structural._
  import math._
  import LogicalPlan._
  import SemanticAnalysis._
}