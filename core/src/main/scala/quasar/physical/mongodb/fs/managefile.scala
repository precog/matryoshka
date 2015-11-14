package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp.TaskRef
import quasar.fs._

import com.mongodb.{MongoCommandException, MongoServerException}
import com.mongodb.async.client.MongoClient

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

object managefile {
  import ManageFile._, FileSystemError._, PathError2._, MongoDb._, fsops._

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

  /** Interpret `ManageFile` using MongoDB. */
  val interpret: ManageFile ~> MongoManage = new (ManageFile ~> MongoManage) {
    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageInT]

      case Delete(path) =>
        path.fold(deleteDir, deleteFile)
          .run.liftM[ManageInT]

      case TempFile(maybeNear) =>
        val dbName = defaultDb map { defDb =>
          maybeNear
            .flatMap(f => Collection.fromFile(f).toOption)
            .cata(_.databaseName, defDb.run)
        }

        (defaultDb |@| freshName)((db, n) => rootDir </> dir(db.run) </> file(n))
    }
  }

  /** Run [[MongoManage]], given a `MongoClient` and the name of a database
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

  private type R[S, A] = Kleisli[MongoDb, S, A]

  private def moveToRename(sem: MoveSemantics): RenameSemantics = {
    import RenameSemantics._
    sem.fold(Overwrite, FailIfExists, Overwrite)
  }

  private def moveDir(src: AbsDir[Sandboxed], dst: AbsDir[Sandboxed], sem: MoveSemantics)
                     : MongoFsM[Unit] = {
    for {
      colls    <- collectionsInDir(src)
      srcFiles =  colls map (_.asFile)
      dstFiles =  srcFiles.map(_ relativeTo (src) map (dst </> _)).unite
      _        <- srcFiles zip dstFiles traverseU {
                    case (s, d) => moveFile(s, d, sem)
                  }
    } yield ()
  }

  private def moveFile(src: AbsFile[Sandboxed], dst: AbsFile[Sandboxed], sem: MoveSemantics)
                      : MongoFsM[Unit] = {

    // TODO: Is there a more structured indicator for these errors, the code
    //       appears to be '-1', which is suspect.
    val srcNotFoundErr = "source namespace does not exist"
    val dstExistsErr = "target namespace exists"

    /** Error codes obtained from MongoDB `renameCollection` docs:
      * See http://docs.mongodb.org/manual/reference/command/renameCollection/
      */
    def reifyMongoErr(m: MongoDb[Unit]): MongoFsM[Unit] =
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

    def ensureDstExists(dstColl: Collection): MongoFsM[Unit] =
      EitherT(collectionsIn(dstColl.databaseName)
                .filter(_ == dstColl)
                .runLast
                .map(_.toRightDisjunction(PathError(FileNotFound(dst))).void))

    if (src == dst)
      collFromFileM(src) flatMap (srcColl =>
        collectionExists(srcColl).liftM[FileSystemErrT].ifM(
          sem.fold(
            ().point[MongoFsM],
            MonadError[MongoE, FileSystemError].raiseError(PathError(FileExists(src))),
            ().point[MongoFsM]),
          MonadError[MongoE, FileSystemError].raiseError(PathError(FileNotFound(src)))))
    else
      for {
        srcColl <- collFromFileM(src)
        dstColl <- collFromFileM(dst)
        rSem    =  moveToRename(sem)
        _       <- sem.fold(().point[MongoFsM], ().point[MongoFsM], ensureDstExists(dstColl))
        _       <- reifyMongoErr(rename(srcColl, dstColl, rSem))
      } yield ()
  }

  // TODO: Really need a Path#fold[A] method, which will be much more reliable
  //       than this process of deduction.
  private def deleteDir(dir: AbsDir[Sandboxed]): MongoFsM[Unit] =
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

  private def deleteFile(file: AbsFile[Sandboxed]): MongoFsM[Unit] =
    collFromFileM(file) flatMap (c =>
      collectionExists(c).liftM[FileSystemErrT].ifM(
        dropCollection(c).liftM[FileSystemErrT],
        PathError(FileNotFound(file)).raiseError[MongoE, Unit]))

  private def defaultDb: MongoManage[DefaultDb] =
    MonadReader[R, ManageIn].ask.map(_._1)

  private def freshName: MongoManage[String] =
    for {
      in <- MonadReader[R, ManageIn].ask
      (_, prefix, ref) = in
      n  <- liftTask(ref.modifyS(i => (i + 1, i))).liftM[ManageInT]
    } yield prefix.run + n

  private def tmpPrefix: Task[TmpPrefix] =
    NameGenerator.salt map (s => TmpPrefix(s"__quasar.tmp_${s}_"))
}
