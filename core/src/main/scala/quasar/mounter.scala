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
import quasar.Errors._
import quasar.Evaluator._
import quasar.config._
import quasar.fs._

import scalaz._
import scalaz.concurrent._

object Mounter {
  def defaultMount(config: Config): ETask[EnvironmentError, Backend] =
    mount(config, BackendDefinitions.All)

  def mount(config: Config, backendDef: BackendDefinition): ETask[EnvironmentError, Backend] = {
    def rec(backend: Backend, path: List[DirNode], conf: BackendConfig): ETask[EnvironmentError, Backend] =
      backend match {
        case NestedBackend(base) =>
          path match {
            case Nil => backendDef(conf).fold[EitherT[Task, EnvironmentError, Backend]](
              EitherT.left(Task.now(MissingFileSystem(Path(path, None), conf))))(
              EitherT.right)
            case dir :: dirs =>
              rec(base.get(dir).getOrElse(NestedBackend(Map())), dirs, conf).map(rez => NestedBackend(base + (dir -> rez)))
          }
        case _ => EitherT.left(Task.now(InvalidConfig("attempting to mount a backend within an existing backend.")))
      }

    config.mountings.foldLeft[ETask[EnvironmentError, Backend]](
      EitherT.right(Task.now(NestedBackend(Map())))) {
      case (root, (path, config)) =>
        root.flatMap(rec(_, path.asAbsolute.asDir.dir, config))
    }
  }
}
