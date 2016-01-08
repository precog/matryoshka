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

package quasar.physical.mongodb.fs

import quasar.Predef._
import quasar.fp.TaskRef
import quasar.fs._
import quasar.physical.mongodb._
import quasar.physical.mongodb.fs.bsoncursor._

import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {
  import ReadFile._, FileSystemError._, PathError2._, MongoDbIO._

  type ReadState           = (Long, Map[ReadHandle, BsonCursor])
  type ReadStateT[F[_], A] = ReaderT[F, TaskRef[ReadState], A]
  type MongoRead[A]        = ReadStateT[MongoDbIO, A]

  /** Interpret the `ReadFile` algebra using MongoDB */
  val interpret: ReadFile ~> MongoRead = new (ReadFile ~> MongoRead) {
    val DC = DataCursor[MongoDbIO, BsonCursor]

    def apply[A](rf: ReadFile[A]) = rf match {
      case Open(file, offset, limit) =>
        openCursor(file, offset, limit)

      case Read(h) =>
        lookupCursor(h)
          .flatMapF(c => DC.nextChunk(c).liftM[ReadStateT])
          .toRight(UnknownReadHandle(h))
          .run

      case Close(h) =>
        OptionT[MongoRead, BsonCursor](MongoRead(cursorL(h) <:= None))
          .flatMapF(c => DC.close(c).liftM[ReadStateT])
          .run.void
    }
  }

  /** Run [[MongoRead]], using the given `MongoClient` in the `Task` monad. */
  def run(client: MongoClient): Task[MongoRead ~> Task] =
    TaskRef[ReadState]((0, Map.empty)) map { ref =>
      new (MongoRead ~> Task) {
        def apply[A](rm: MongoRead[A]) = rm.run(ref).run(client)
      }
    }

  ////

  private def MongoRead[A](f: TaskRef[ReadState] => Task[A]): MongoRead[A] =
    Kleisli(ref => liftTask(f(ref)))

  private def MongoRead[A](s: State[ReadState, A]): MongoRead[A] =
    MongoRead(_ modifyS s.run)

  private val seqL: ReadState @> Long =
    Lens.firstLens

  private val cursorsL: ReadState @> Map[ReadHandle, BsonCursor] =
    Lens.secondLens

  private def cursorL(h: ReadHandle): ReadState @> Option[BsonCursor] =
    Lens.mapVLens(h) compose cursorsL

  private def readState: MongoRead[ReadState] =
    MongoRead(_.read)

  private def freshHandle(f: AFile): MongoRead[ReadHandle] =
    MongoRead(seqL <%= (_ + 1)) map (ReadHandle(f, _))

  private def recordCursor(f: AFile, c: BsonCursor): MongoRead[ReadHandle] =
    freshHandle(f) flatMap (h => MongoRead(cursorL(h) := Some(c)) as h)

  private def lookupCursor(h: ReadHandle): OptionT[MongoRead, BsonCursor] =
    OptionT[MongoRead, BsonCursor](readState map (cursorL(h).get))

  private def openCursor(
    f: AFile,
    off: Natural,
    lim: Option[Positive]
  ): MongoRead[FileSystemError \/ ReadHandle] = {
    def openCursor0(c: Collection): MongoRead[ReadHandle] =
      for {
        it   <- find(c).liftM[ReadStateT]
        skpd =  it skip off.value.toInt
        ltd  =  lim cata (n => skpd.limit(n.value.toInt), skpd)
        cur  <- async(ltd.batchCursor).liftM[ReadStateT]
        h    <- recordCursor(f, cur)
      } yield h

    Collection.fromPathy(f).fold(
      err  => PathError(err).left.point[MongoRead],
      coll => collectionExists(coll).liftM[ReadStateT].ifM(
                openCursor0(coll) map (_.right[FileSystemError]),
                PathError(PathNotFound(f)).left.point[MongoRead]))
  }
}
