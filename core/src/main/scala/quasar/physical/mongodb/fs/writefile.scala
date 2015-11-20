package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp._
import quasar.fs._

import org.bson.Document
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {
  import WriteFile._, FileSystemError._, MongoDbIO._

  type WriteState           = (Long, Map[WriteHandle, Collection])
  type WriteStateT[F[_], A] = ReaderT[F, TaskRef[WriteState], A]
  type MongoWrite[A]        = WriteStateT[MongoDbIO, A]

  /** Interpret the `WriteFile` algebra using MongoDB. */
  val interpret: WriteFile ~> MongoWrite = new (WriteFile ~> MongoWrite) {
    def apply[A](wf: WriteFile[A]) = wf match {
      case Open(file) =>
        Collection.fromPathy(file) fold (
          err => PathError(err).left.point[MongoWrite],
          col => ensureCollection(col).liftM[WriteStateT] *>
                 recordCollection(col) map \/.right)

      case Write(h, data) =>
        val (errs, docs) = data foldMap { d =>
          dataToDocument(d).fold(
            e => (Vector(e), Vector()),
            d => (Vector(), Vector(d)))
        }

        lookupCollection(h) flatMap (_ cata (
          c => insertAny(c, docs)
                 .filter(_ < docs.size)
                 .map(n => PartialWrite(docs.size - n))
                 .run.map(errs ++ _.toList)
                 .liftM[WriteStateT],
          (errs :+ UnknownWriteHandle(h)).point[MongoWrite]))

      case Close(h) =>
        MongoWrite(collectionL(h) := None).void
    }
  }

  /** Run [[MongoWrite]] using the given `MongoClient` in the `Task`
    * monad.
    */
  def run(client: MongoClient): Task[MongoWrite ~> Task] =
    TaskRef[WriteState]((0, Map.empty)) map { ref =>
      new (MongoWrite ~> Task) {
        def apply[A](wm: MongoWrite[A]) = wm.run(ref).run(client)
      }
    }

  ////

  private def MongoWrite[A](f: TaskRef[WriteState] => Task[A]): MongoWrite[A] =
    Kleisli(ref => liftTask(f(ref)))

  private def MongoWrite[A](s: State[WriteState, A]): MongoWrite[A] =
    MongoWrite(_ modifyS s.run)

  private val seqL: WriteState @> Long =
    Lens.firstLens

  private val collectionsL: WriteState @> Map[WriteHandle, Collection] =
    Lens.secondLens

  private def collectionL(h: WriteHandle): WriteState @> Option[Collection] =
    Lens.mapVLens(h) compose collectionsL

  private def writeState: MongoWrite[WriteState] =
    MongoWrite(_.read)

  private def freshHandle: MongoWrite[WriteHandle] =
    MongoWrite(seqL <%= (_ + 1)) map (WriteHandle(_))

  private def recordCollection(c: Collection): MongoWrite[WriteHandle] =
    freshHandle flatMap (h => MongoWrite(collectionL(h) := Some(c)) as h)

  private def lookupCollection(h: WriteHandle): MongoWrite[Option[Collection]] =
    writeState map (collectionL(h).get)

  private def dataToDocument(d: Data): FileSystemError \/ Document =
    BsonCodec.fromData(d)
      .leftMap(err => WriteFailed(d, err.toString))
      .flatMap {
        case doc @ Bson.Doc(_) => doc.repr.right
        case otherwise         => WriteFailed(d, "MongoDB is only able to store documents").left
      }
}
