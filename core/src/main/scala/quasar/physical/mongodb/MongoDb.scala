package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._

import scala.collection.JavaConverters._
import java.util.LinkedList

import org.bson.Document
import com.mongodb.{MongoNamespace, MongoCredential, MongoCommandException}
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model._
import com.mongodb.async._
import com.mongodb.async.client._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

final class MongoDb[A] private (protected val r: ReaderT[Task, MongoClient, A]) {
  def map[B](f: A => B): MongoDb[B] =
    new MongoDb(r map f)

  def flatMap[B](f: A => MongoDb[B]): MongoDb[B] =
    new MongoDb(r flatMap (a => f(a).r))

  def attempt: MongoDb[Throwable \/ A] =
    new MongoDb(r mapK (_.attempt))

  def run(c: MongoClient): Task[A] = r.run(c)
}

object MongoDb {

  def collectionExists(c: Collection): MongoDb[Boolean] =
    collectionsIn(c.databaseName)
      .exists(_.collectionName == c.collectionName)
      .runLastOr(false)

  /** All discoverable collections on the server. */
  def collections: Process[MongoDb, Collection] =
    databaseNames flatMap collectionsIn

  /** The collections in the named database. */
  def collectionsIn(dbName: String): Process[MongoDb, Collection] =
    database(dbName).liftM[Process]
      .flatMap(db => iterableToProcess(db.listCollectionNames))
      .map(Collection(dbName, _))

  /** Creates the given collection. */
  def createCollection(c: Collection): MongoDb[Unit] =
    database(c.databaseName) flatMap (db =>
      async[java.lang.Void](db.createCollection(c.collectionName, _)).void)

  /** Names of all discoverable databases on the server. */
  def databaseNames: Process[MongoDb, String] =
    client.liftM[Process]
      .flatMap(c => iterableToProcess(c.listDatabaseNames))
      .onFailure {
        case t: MongoCommandException =>
          credentials.liftM[Process]
            .flatMap(ys => Process.emitAll(ys.map(_.getSource).distinct))

        case t =>
          Process.fail(t)
      }

  def dropCollection(c: Collection): MongoDb[Unit] =
    collection(c) flatMap (mc => async(mc.drop).void)

  def dropDatabase(named: String): MongoDb[Unit] =
    database(named) flatMap (d => async(d.drop).void)

  def dropAllDatabases: MongoDb[Unit] =
    databaseNames.map(dropDatabase).eval.run

  /** Ensure the given collection exists, creating it if not. */
  def ensureCollection(c: Collection): MongoDb[Unit] =
    collectionExists(c).ifM(().point[MongoDb], createCollection(c))

  /** Returns the name of the first database where an insert to the collection
    * having the given name succeeds.
    */
  def firstWritableDb(collName: String): OptionT[MongoDb, String] = {
    type M[A] = OptionT[MongoDb, A]

    val testDoc = Bson.Doc(ListMap("a" -> Bson.Int32(1))).repr

    def canWriteToCol(coll: Collection): M[String] =
      insertAny[Id](testDoc, coll)
        .filter(_ == 1)
        .as(coll.databaseName)
        .attempt
        .flatMap(r => OptionT(r.toOption.point[MongoDb]))

    databaseNames
      .translate[M](liftMT[MongoDb, OptionT])
      .map(n => canWriteToCol(Collection(n, collName)))
      .eval.take(1).runLast
      .flatMap(n => OptionT(n.point[MongoDb]))
  }

  /** Attempts to insert as many of the given documents into the collection as
    * possible. The number of documents inserted is returned, if possible, and
    * may be smaller than the original amount if any documents failed to insert.
    */
  def insertAny[F[_]: Foldable](docs: F[Document], coll: Collection): OptionT[MongoDb, Int] = {
    val docList = new LinkedList[WriteModel[Document]]
    val writeOpts = (new BulkWriteOptions()).ordered(false)

    Foldable[F].traverse_(docs)(d => docList.add(new InsertOneModel(d)): Id[Boolean])

    if (docList.isEmpty)
      OptionT.none
    else
      OptionT(collection(coll)
        .flatMap(c => async[BulkWriteResult](c.bulkWrite(docList, writeOpts, _)))
        .map(r => r.wasAcknowledged option r.getInsertedCount))
  }

  /** Rename `src` to `dst` using the given semantics. */
  def rename(src: Collection, dst: Collection, semantics: RenameSemantics): MongoDb[Unit] = {
    import RenameSemantics._

    val dropDst = semantics match {
      case Overwrite    => true
      case FailIfExists => false
    }

    if (src == dst)
      ().point[MongoDb]
    else
      collection(src)
        .flatMap(c => async[java.lang.Void](c.renameCollection(
          new MongoNamespace(dst.databaseName, dst.collectionName),
          (new RenameCollectionOptions) dropTarget dropDst,
          _)))
        .void
  }

  def fail[A](t: Throwable): MongoDb[A] =
    liftTask(Task.fail(t))

  val liftTask: Task ~> MongoDb =
    new (Task ~> MongoDb) {
      def apply[A](t: Task[A]) = lift(_ => t)
    }

  private[mongodb] def find(c: Collection): MongoDb[FindIterable[Document]] =
    collection(c) map (_.find)

  private[mongodb] def async[A](f: SingleResultCallback[A] => Unit): MongoDb[A] =
    liftTask(Task.async(cb => f(new DisjunctionCallback(cb))))

  implicit val mongoDbInstance: Monad[MongoDb] with Catchable[MongoDb] =
    new Monad[MongoDb] with Catchable[MongoDb] {
      override def map[A, B](fa: MongoDb[A])(f: A => B) = fa map f
      def point[A](a: => A) = new MongoDb(Kleisli(_ => Task.now(a)))
      def bind[A, B](fa: MongoDb[A])(f: A => MongoDb[B]) = fa flatMap f
      def fail[A](t: Throwable) = fail(t)
      def attempt[A](fa: MongoDb[A]) = fa.attempt
    }

  ////

  private def apply[A](f: MongoClient => A): MongoDb[A] =
    lift(c => Task.delay(f(c)))

  private def lift[A](f: MongoClient => Task[A]): MongoDb[A] =
    new MongoDb(Kleisli(f))

  private def client: MongoDb[MongoClient] =
    MongoDb(ι)

  // TODO: Make a basic credential type in scala and expose this method.
  private val credentials: MongoDb[List[MongoCredential]] =
    MongoDb(_.getSettings.getCredentialList.asScala.toList)

  private def collection(c: Collection): MongoDb[MongoCollection[Document]] =
    database(c.databaseName) map (_ getCollection c.collectionName)

  private def database(named: String): MongoDb[MongoDatabase] =
    MongoDb(_ getDatabase named)

  private def iterableToProcess[A](it: MongoIterable[A]): Process[MongoDb, A] = {
    def go(c: AsyncBatchCursor[A]): Process[MongoDb, A] =
      Process.eval(async(c.next))
        .flatMap(r => Option(r).cata(
          as => Process.emitAll(as.asScala.toVector) ++ go(c),
          Process.halt))

    Process.eval(async(it.batchCursor)) flatMap (cur =>
      go(cur) onComplete Process.eval_(MongoDb(_ => cur.close())))
  }

  private final class DisjunctionCallback[A](f: Throwable \/ A => Unit)
    extends SingleResultCallback[A] {

    def onResult(result: A, error: Throwable): Unit =
      f(Option(error) <\/ result)
  }
}
