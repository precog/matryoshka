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
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fs._

import pathy.Path._
import scalaz.{Node => _, _}
import scalaz.syntax.monad._
import scalaz.syntax.monadError._

object fsops {
  type MongoFsM[A]  = FileSystemErrT[MongoDbIO, A]
  type MongoE[A, B] = EitherT[MongoDbIO, A, B]

  import FileSystemError._, PathError2._

  /** The collections having a prefix equivalent to the given directory path. */
  def collectionsInDir(dir: ADir): MongoFsM[Vector[Collection]] =
    for {
      c  <- collFromPathM(dir)
      cs <- MongoDbIO.collectionsIn(c.databaseName)
              .filter(_.collectionName startsWith c.collectionName)
              .runLog.map(_.toVector).liftM[FileSystemErrT]
      _  <- if (cs.isEmpty) PathError(PathNotFound(dir)).raiseError[MongoE, Unit]
            else ().point[MongoFsM]
    } yield cs

  /** A filesystem `Node` representing the first segment of a collection name
    * relative to the given parent directory.
    */
  def collectionToNode(parent: ADir): Collection => Option[Node] =
    _.asFile relativeTo parent flatMap Node.fromFirstSegmentOf

  /** The collection represented by the given path. */
  def collFromPathM(path: APath): MongoFsM[Collection] =
    EitherT(Collection.fromPathy(path).leftMap(PathError).point[MongoDbIO])

  /** An error indicating that the directory refers to an ancestor of `/`.
    *
    * TODO: This would be eliminated if we switched to AbsDir everywhere and
    *       disallowed AbsDirs like "/../foo" by construction. Revisit this once
    *       scala-pathy has been updated.
    */
  def nonExistentParent[A](dir: ADir): MongoFsM[A] =
    PathError(InvalidPath(dir, "directory refers to nonexistent parent"))
      .raiseError[MongoE, A]
}
