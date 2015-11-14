package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fs._

import pathy.Path._

import scalaz.{Node => _, _}
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.monadError._

object fsops {
  type MongoFsM[A]  = FileSystemErrT[MongoDb, A]
  type MongoE[A, B] = EitherT[MongoDb, A, B]

  import FileSystemError._, PathError2._

  /** The collections having a prefix equivalent to the given directory path. */
  def collectionsInDir(dir: AbsDir[Sandboxed]): MongoFsM[Vector[Collection]] =
    for {
      c  <- collFromDirM(dir)
      cs <- MongoDb.collectionsIn(c.databaseName)
              .filter(_.collectionName startsWith c.collectionName)
              .runLog.map(_.toVector).liftM[FileSystemErrT]
      _  <- if (cs.isEmpty) PathError(DirNotFound(dir)).raiseError[MongoE, Unit]
            else ().point[MongoFsM]
    } yield cs

  /** A filesystem `Node` representing the first segment of a collection name
    * relative to the given parent directory.
    */
  def collectionToNode(parent: AbsDir[Sandboxed]): Collection => Option[Node] =
    _.asFile relativeTo parent flatMap Node.fromFirstSegmentOf

  /** The collection represented by the given directory. */
  def collFromDirM(dir: AbsDir[Sandboxed]): MongoFsM[Collection] =
    EitherT(Collection.fromDir(dir).leftMap(PathError).point[MongoDb])

  /** The collection represented by the given file. */
  def collFromFileM(file: AbsFile[Sandboxed]): MongoFsM[Collection] =
    EitherT(Collection.fromFile(file).leftMap(PathError).point[MongoDb])

  /** An error indicating that the directory refers to an ancestor of `/`.
    *
    * TODO: This would be eliminated if we switched to AbsDir everywhere and
    *       disallowed AbsDirs like "/../foo" by construction. Revisit this once
    *       scala-pathy has been updated.
    */
  def nonExistentParent[A](dir: AbsDir[Sandboxed]): MongoFsM[A] =
    PathError(InvalidPath(dir.left, "directory refers to nonexistent parent"))
      .raiseError[MongoE, A]
}
