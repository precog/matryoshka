package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp.TaskRef
import quasar.fs._

import scala.collection.JavaConverters._

import org.bson.Document
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {
  import ReadFile._, FileSystemError._, PathError2._, MongoDb._

  type BsonCursor          = AsyncBatchCursor[Document]
  type ReadState           = (Long, Map[ReadHandle, BsonCursor])
  type ReadStateT[F[_], A] = ReaderT[F, TaskRef[ReadState], A]
  type MongoRead[A]        = ReadStateT[MongoDb, A]

  /** Interpret the `ReadFile` algebra using MongoDB */
  val interpret: ReadFile ~> MongoRead = new (ReadFile ~> MongoRead) {
    def apply[A](rf: ReadFile[A]) = rf match {
      case Open(file, offset, limit) =>
        Collection.fromFile(file).fold(
          err  => PathError(err).left.point[MongoRead],
          coll => collectionExists(coll).liftM[ReadStateT].ifM(
                    openCursor(coll, offset, limit) map (_.right[FileSystemError]),
                    PathError(FileNotFound(file)).left.point[MongoRead]))

      case Read(h) =>
        lookupCursor(h)
          .flatMapF(nextChunk)
          .toRight(UnknownReadHandle(h))
          .run

      case Close(h) =>
        OptionT[MongoRead, BsonCursor](MongoRead(cursorL(h) <:= None))
          .flatMapF(c => liftTask(Task.delay(c.close())).liftM[ReadStateT])
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

  private def freshHandle: MongoRead[ReadHandle] =
    MongoRead(seqL <%= (_ + 1)) map (ReadHandle(_))

  private def recordCursor(c: BsonCursor): MongoRead[ReadHandle] =
    freshHandle flatMap (h => MongoRead(cursorL(h) := Some(c)) as h)

  private def lookupCursor(h: ReadHandle): OptionT[MongoRead, BsonCursor] =
    OptionT[MongoRead, BsonCursor](readState map (cursorL(h).get))

  private def openCursor(c: Collection, off: Natural, lim: Option[Positive]): MongoRead[ReadHandle] =
    for {
      it   <- find(c).liftM[ReadStateT]
      skpd =  it skip off.run.toInt
      ltd  =  lim cata (n => skpd.limit(n.run.toInt), skpd)
      cur  <- async(ltd.batchCursor).liftM[ReadStateT]
      h    <- recordCursor(cur)
    } yield h

  private def nextChunk(c: BsonCursor): MongoRead[Vector[Data]] = {
    val withoutId: Document => Document =
      d => (d: Id[Document]) map (_ remove "_id") as d

    val toData: Document => Data =
      (BsonCodec.toData _) compose (Bson.fromRepr _) compose withoutId

    // NB: `null` is used as a sentinel value to indicate input is exhausted,
    //     because Java.
    async(c.next)
      .map(r => Option(r).map(_.asScala.toVector).orZero.map(toData))
      .liftM[ReadStateT]
  }
}
