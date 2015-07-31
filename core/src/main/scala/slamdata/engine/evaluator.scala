/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine

import slamdata.Predef._

import scalaz._
import scalaz.concurrent.Task

import slamdata.engine.fs._

final case class EvaluationError(cause: Throwable) extends Error {
  def message = "An error occurred during evaluation: " + cause.toString
}

sealed trait ResultPath {
  def path: Path
}
object ResultPath {
  /** Path to a result which names an unaltered source resource or the requested destination. */
  final case class User(path: Path) extends ResultPath

  /** Path to a result which is a new temporary resource created during query execution. */
  final case class Temp(path: Path) extends ResultPath
}

trait Evaluator[PhysicalPlan] {
  /**
   * Executes the specified physical plan.
   *
   * Returns the location where the output results are located. In some
   * cases (e.g. SELECT * FROM FOO), this may not be equal to the specified
   * destination resource (because this would require copying all the data).
   */
  def execute(physical: PhysicalPlan): Task[ResultPath]

  /**
   * Compile the specified physical plan to a command
   * that can be run natively on the backend.
   */
  def compile(physical: PhysicalPlan): (String, Cord)

  /**
   * Fails if the backend implementation is not compatible with the connected
   * system (typically because it does not have not the correct version number).
   */
  def checkCompatibility: Task[Error \/ Unit]
}
