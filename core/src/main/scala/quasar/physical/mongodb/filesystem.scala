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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar._, Backend._, Errors._
import quasar.fs._, Path._

import scalaz._, Scalaz._
import scalaz.concurrent._
import scalaz.stream._

trait MongoDbFileSystem extends PlannerBackend[Workflow.Crystallized] {
  protected def server: MongoWrapper

  val ChunkSize = 1000

  def scan0(path: Path, offset: Long, limit: Option[Long]):
      Process[ETask[ResultError, ?], Data] = {
    val skipper = (it: com.mongodb.client.FindIterable[org.bson.Document]) => it.skip(offset.toInt)
    // NB As per the MondoDB documentation, we inverse the value of limit in order to retrieve a single batch.
    // The documentation around this is very confusing, but it would appear that proceeding this way allows us
    // to retrieve the data in a single batch. Behavior of the limit parameter on large datasets is tested in
    // FileSystemSpecs and appears to work as expected.
    val limiter = (it: com.mongodb.client.FindIterable[org.bson.Document]) => limit.map(v => it.limit(-v.toInt)).getOrElse(it)

    val skipperAndLimiter = skipper andThen limiter

    Collection.fromPath(path).fold(
      e => Process.eval[ETask[ResultError, ?], Data](EitherT.left(Task.now(ResultPathError(e)))),
      col => server.readCursor(server.get(col).map(c => skipperAndLimiter(c.find()))))
  }

  def count0(path: Path): ProcessingTask[Long] = {
    val p = swapT(Collection.fromPath(path).map(server.get(_).map(_.count)))
    convertError(PPathError.apply)(p)
  }

  def save0(path: Path, values: Process[Task, Data]): ProcessingTask[Unit] =
    Collection.fromPath(path).fold(
      e => EitherT.left(Task.now(PPathError(e))),
      col => for {
        tmp <- liftP(server.genTempName(col)).leftMap(PPathError(_))
        _   <- append(tmp.asPath, values).runLog.leftMap(PPathError(_)).flatMap(_.headOption.fold[ProcessingTask[Unit]](
          ().point[ProcessingTask])(
          e => new CatchableOps[ProcessingTask, Unit] { val self = delete(tmp.asPath).leftMap(PPathError(_)) }.ignoreAndThen(EitherT.left(Task.now(PWriteError(e))))))
        _   <- delete(path).leftMap(PPathError(_))
        _   <- new CatchableOps[PathTask, Unit] { val self =  liftE[PathError](server.rename(tmp, col, RenameSemantics.FailIfExists)) }.onFailure(delete(tmp.asPath)).leftMap(PPathError(_))
      } yield ())

  def append0(path: Path, values: Process[Task, Data]):
      Process[PathTask, WriteError] =
    Collection.fromPath(path).fold(
      e => Process.eval[PathTask, WriteError](EitherT.left(Task.now(e))),
      col => {
        import process1._

        val chunks: Process[Task, Vector[(Data, String \/ org.bson.Document)]] = {
          def unwrap(obj: Bson) = obj match {
            case doc @ Bson.Doc(_) => \/-(doc.repr)
            case value => -\/("Cannot store value in MongoDB: " + value)
          }
          values.map(json => json -> BsonCodec.fromData(json).fold(err => -\/(err.message), unwrap)) pipe chunk(ChunkSize)
        }

        chunks.flatMap { vs =>
          Process.eval(Task.delay {
            val parseErrors = vs.collect { case (json, -\/ (err)) => WriteError(json, Some(err)) }
            val objs        = vs.collect { case (json,  \/-(obj)) => json -> obj }

            val insertErrors = server.insert(col, objs.map(_._2)).attemptRun.fold(
              e => objs.map { case (json, _) => WriteError(json, Some(e.getMessage)) },
              _ => Nil)

            parseErrors ++ insertErrors
          }).flatMap(errs => Process.emitAll(errs))
        }
      }.translate(liftP))

  def move0(src: Path, dst: Path, semantics: MoveSemantics): PathTask[Unit] = {
    val rs = RenameSemantics(semantics)

    def target(col: Collection): Option[Collection] =
      (for {
        rel <- col.asPath rebase src
        p = dst.asDir ++ rel
        c   <- Collection.fromPath(p)
      } yield c).toOption

    Collection.fromPath(dst).fold(
      e => EitherT.left(Task.now(e)),
      dstCol =>
      if (src.pureDir)
        liftP(for {
          cols    <- server.collections
          renames <- cols.map(s => target(s).map(server.rename(s, _, rs))).foldMap(_.toList).sequenceU
        } yield ())
      else
        Collection.fromPath(src).fold(
          e => EitherT.left(Task.now(e)),
          srcCol => liftP(server.rename(srcCol, dstCol, rs))))
  }

  def delete0(path: Path): PathTask[Unit] =
    swapT(Collection.foldPath(path)(
      server.dropAllDatabases,
      server.dropDatabase,
      κ(for {
        all     <- server.collections
        deletes <- all.map(col => col.asPath.rebase(path.dirOf).fold(
          κ(().point[Task]),
          file =>  {
            if (path.file == None || path.fileOf == file)
              server.drop(col)
            else ().point[Task]
          })).sequenceU
      } yield ())))

  // Note: a mongo db can contain a collection named "foo" as well as "foo.bar"
  // and "foo.baz", in which case "foo" acts as both a directory and a file, as
  // far as Quasar is concerned.
  def ls0(dir: Path): PathTask[Set[FilesystemNode]] = liftP(for {
    cols <- server.collections
    allPaths = cols.map(_.asPath)
  } yield allPaths.map(_.rebase(dir).toOption.map(p => FilesystemNode(p.head, None))).foldMap(_.toSet))
}

sealed trait RenameSemantics
object RenameSemantics {
  case object Overwrite extends RenameSemantics
  case object FailIfExists extends RenameSemantics

  def apply(ms: MoveSemantics) = ms match {
    case Backend.Overwrite => Overwrite
    case Backend.FailIfExists => FailIfExists
  }
}
