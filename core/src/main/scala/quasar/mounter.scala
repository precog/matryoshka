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
import quasar.Evaluator._
import quasar.config._
import quasar.fs._

import scalaz._, Scalaz._
import scalaz.concurrent._

object Mounter {
  def defaultMount(config: Config): EnvTask[Backend] =
    mount(config, BackendDefinitions.All)

  def mount(config: Config, backendDef: BackendDefinition): EnvTask[Backend] = {
    def rec0(backend: Backend, path: List[DirNode], conf: BackendConfig): EnvTask[Backend] =
      backend match {
        case NestedBackend(base) => path match {
          case Nil =>
            backendDef(conf).leftMap {
              case MissingBackend(_) => MissingFileSystem(Path(path, None), conf)
              case otherwise         => otherwise
            }

          case dir :: dirs =>
            rec0(base.get(dir).getOrElse(NestedBackend(Map())), dirs, conf)
              .map(rez => NestedBackend(base + (dir -> rez)))
        }

        case _ => EitherT.left(Task.now(InvalidConfig("attempting to mount a backend within an existing backend.")))
      }

    def rec(backend: Backend, mount: (Path, BackendConfig)): EnvTask[Backend] = mount match {
      case (path, config) => rec0(backend, path.removeCurrentDir.asDir.dir, config)
    }

    config.mountings.toList.foldLeftM(NestedBackend(Map()): Backend)(rec)
  }
}
