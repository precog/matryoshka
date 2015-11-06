package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp.TaskRef
import quasar.fs._

import com.mongodb.{MongoCommandException, MongoServerException}
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import pathy.Path._

object managefile {
  import ManageFile._, FileSystemError._, PathError2._, MongoDb._

  type ManageIn           = (DefaultDb, TmpPrefix, TaskRef[Long])
  type ManageInT[F[_], A] = ReaderT[F, ManageIn, A]
  type MongoManage[A]     = ManageInT[MongoDb, A]

  /** TODO: There are still some questions regarding Path
    *   1) We should assume all paths will be canonicalized and can do so
    *      with a ManageFile ~> ManageFile that canonicalizes everything.
    *
    *   2) Currently, parsing a directory like "/../foo/bar/" as an absolute
    *      dir succeeds, this should probably be changed to fail.
    */

  /** Interpret [[ManageFile]] using MongoDB. */
  val interpret: ManageFile ~> MongoManage = new (ManageFile ~> MongoManage) {
    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageInT]

      case Delete(path) =>
        path.fold(deleteDir, deleteFile)
          .run.liftM[ManageInT]

      case ListContents(dir) =>
        (dirName(dir) match {
          case Some(_) =>
            collectionsInDir(dir)
              .map(_ foldMap (collectionToNode(dir) andThen (_.toSet)))
              .run

          case None if depth(dir) == 0 =>
            collections
              .map(collectionToNode(dir))
              .pipe(process1.stripNone)
              .runLog
              .map(_.toSet.right[FileSystemError])

          case None =>
            nonExistentParent[Set[Node]](dir).run
        }).liftM[ManageInT]

      case TempFile(maybeNear) =>
        val dbName = defaultDb map { defDb =>
          maybeNear
            .flatMap(f => Collection.fromFile(f).toOption)
            .cata(_.databaseName, defDb.run)
        }

        (defaultDb |@| freshName)((db, n) => rootDir </> dir(db.run) </> file(n))
    }
  }

  /** Run [[MongoManage]], given a [[MongoClient]] and the name of a database
    * to use as a default location for temp collections when no other database
    * can be deduced.
    */
  def run(client: MongoClient, defDb: DefaultDb): Task[MongoManage ~> Task] =
    (tmpPrefix |@| TaskRef(0L)) { (prefix, ref) =>
      new (MongoManage ~> Task) {
        def apply[A](fs: MongoManage[A]) =
          fs.run((defDb, prefix, ref)).run(client)
      }
    }

  ////

  private type M[A] = FileSystemErrT[MongoDb, A]
  private type G[E, A] = EitherT[MongoDb, E, A]
  private type R[S, A] = Kleisli[MongoDb, S, A]

  private def moveToRename(sem: MoveSemantics): RenameSemantics = {
    import RenameSemantics._
    sem.fold(Overwrite, FailIfExists, Overwrite)
  }

  private def moveDir(src: AbsDir[Sandboxed], dst: AbsDir[Sandboxed], sem: MoveSemantics)
                     : M[Unit] = {
    for {
      colls    <- collectionsInDir(src)
      srcFiles =  colls map (_.asFile)
      dstFiles =  srcFiles flatMap (_ relativeTo (src) map (dst </> _))
      _        <- srcFiles zip dstFiles traverseU {
                    case (s, d) => moveFile(s, d, sem)
                  }
    } yield ()
  }

  private def moveFile(src: AbsFile[Sandboxed], dst: AbsFile[Sandboxed], sem: MoveSemantics)
                      : M[Unit] = {

    // TODO: Is there a more structured indicator for these errors, the code
    //       appears to be '-1', which is suspect.
    val srcNotFoundErr = "source namespace does not exist"
    val dstExistsErr = "target namespace exists"

    /** Error codes obtained from MongoDB `renameCollection` docs:
      * See http://docs.mongodb.org/manual/reference/command/renameCollection/
      */
    def reifyMongoErr(m: MongoDb[Unit]): M[Unit] =
      EitherT(m.attempt flatMap {
        case -\/(e: MongoServerException) if e.getCode == 10026 =>
          PathError(FileNotFound(src)).left.point[MongoDb]

        case -\/(e: MongoServerException) if e.getCode == 10027 =>
          PathError(FileExists(dst)).left.point[MongoDb]

        case -\/(e: MongoCommandException) if e.getErrorMessage == srcNotFoundErr =>
          PathError(FileNotFound(src)).left.point[MongoDb]

        case -\/(e: MongoCommandException) if e.getErrorMessage == dstExistsErr =>
          PathError(FileExists(dst)).left.point[MongoDb]

        case -\/(t) =>
          fail(t)

        case \/-(_) =>
          ().right.point[MongoDb]
      })

    def ensureDstExists(dstColl: Collection): M[Unit] =
      EitherT(collectionsIn(dstColl.databaseName)
                .filter(_ == dstColl)
                .runLast
                .map(_.toRightDisjunction(PathError(FileNotFound(dst))).void))

    if (src == dst)
      collFromFileM(src) flatMap (srcColl =>
        collectionExists(srcColl).liftM[FileSystemErrT].ifM(
          sem.fold(
            ().point[M],
            MonadError[G, FileSystemError].raiseError(PathError(FileExists(src))),
            ().point[M]),
          MonadError[G, FileSystemError].raiseError(PathError(FileNotFound(src)))))
    else
      for {
        srcColl <- collFromFileM(src)
        dstColl <- collFromFileM(dst)
        rSem    =  moveToRename(sem)
        _       <- sem.fold(().point[M], ().point[M], ensureDstExists(dstColl))
        _       <- reifyMongoErr(rename(srcColl, dstColl, rSem))
      } yield ()
  }

  // TODO: Really need a Path#fold[A] method, which will be much more reliable
  //       than this process of deduction.
  private def deleteDir(dir: AbsDir[Sandboxed]): M[Unit] =
    dirName(dir) match {
      case Some(n) if depth(dir) == 1 =>
        dropDatabase(n.value).liftM[FileSystemErrT]

      case Some(_) =>
        collectionsInDir(dir)
          .flatMap(_.traverseU_(c => dropCollection(c).liftM[FileSystemErrT]))

      case None if depth(dir) == 0 =>
        dropAllDatabases.liftM[FileSystemErrT]

      case None =>
        nonExistentParent(dir)
    }

  private def deleteFile(file: AbsFile[Sandboxed]): M[Unit] =
    collFromFileM(file) flatMap (c =>
      collectionExists(c).liftM[FileSystemErrT].ifM(
        dropCollection(c).liftM[FileSystemErrT],
        PathError(FileNotFound(file)).raiseError[G, Unit]))

  private def collectionsInDir(dir: AbsDir[Sandboxed]): M[Vector[Collection]] =
    for {
      c  <- collFromDirM(dir)
      cs <- collectionsIn(c.databaseName)
              .filter(_.collectionName startsWith c.collectionName)
              .runLog.map(_.toVector).liftM[FileSystemErrT]
      _  <- if (cs.isEmpty) PathError(DirNotFound(dir)).raiseError[G, Unit]
            else ().point[M]
    } yield cs

  private def collectionToNode(dir: AbsDir[Sandboxed]): Collection => Option[Node] =
    _.asFile relativeTo dir flatMap Node.fromFirstSegmentOf

  private def collFromDirM(dir: AbsDir[Sandboxed]): M[Collection] =
    EitherT(Collection.fromDir(dir).leftMap(PathError).point[MongoDb])

  private def collFromFileM(file: AbsFile[Sandboxed]): M[Collection] =
    EitherT(Collection.fromFile(file).leftMap(PathError).point[MongoDb])

  // TODO: This would be eliminated if we switched to AbsDir everywhere and
  //       disallowed AbsDirs like "/../foo" by construction.
  private def nonExistentParent[A](dir: AbsDir[Sandboxed]): M[A] =
    PathError(InvalidPath(dir.left, "directory refers to nonexistent parent"))
      .raiseError[G, A]

  private def defaultDb: MongoManage[DefaultDb] =
    MonadReader[R, ManageIn].ask.map(_._1)

  private def freshName: MongoManage[String] =
    for {
      in <- MonadReader[R, ManageIn].ask
      (_, prefix, ref) = in
      n  <- liftTask(ref.modifyS(i => (i + 1, i))).liftM[ManageInT]
    } yield prefix.run + n

  private def tmpPrefix: Task[TmpPrefix] =
    Task.delay(scala.util.Random.nextInt().toHexString)
      .map(s => TmpPrefix(s"__quasar.tmp_${s}_"))
}
