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

package quasar

import quasar.Predef._
import quasar.config._
import Evaluator.EnvironmentError
import EnvironmentError.ConnectionFailed

import scalaz.{EitherT, Foldable}
import scalaz.std.list._

object BackendDefinitions {
  val MongoDB: BackendDefinition = BackendDefinition fromPF {
    case config : MongoDbConfig =>
      import quasar.physical.mongodb._
      import Workflow._

      for {
        client    <- EitherT(util.createMongoClient(config).attempt)
                       .leftMap[EnvironmentError](t => ConnectionFailed(t.getMessage))
        mongoEval <- MongoDbEvaluator(client)
      } yield new MongoDbFileSystem {
        val planner = MongoDbPlanner
        val evaluator = mongoEval
        val RP = RenderTree[Crystallized]
        protected def server = MongoWrapper(client)
      }
  }

  val All = Foldable[List].fold(MongoDB :: Nil)
}
