package quasar
package fs

import quasar.Predef._
import quasar.fp._
import quasar.config.{BackendConfig, MongoDbConfig}
import quasar.physical.mongodb.{fs => mongofs}, mongofs.DefaultDb

import com.mongodb.ConnectionString
import com.mongodb.async.client.MongoClients

import monocle.Optional
import monocle.function.Index
import monocle.std.vector._

import org.specs2.mutable.Specification
import org.specs2.execute._
import org.specs2.specification._

import pathy.Path._

import scala.Either

import scalaz.{EphemeralStream => EStream, Optional => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

import FileSystemTest._

abstract class FileSystemTest[S[_]: Functor](
  fss: Task[NonEmptyList[FileSystemUT[S]]])(
  implicit S0: ReadFileF :<: S, S1: WriteFileF :<: S, S2: ManageFileF :<: S)

  extends Specification {

  type F[A]      = Free[S, A]
  type FsTask[A] = FileSystemErrT[Task, A]
  type Run       = F ~> Task

  val read   = ReadFile.Ops[S]
  val write  = WriteFile.Ops[S]
  val manage = ManageFile.Ops[S]

  def fileSystemShould(examples: Run => Unit): Unit =
    fss.map(_ foreach { case FileSystemUT(name, f, prefix) =>
      s"$name FileSystem" should examples(hoistFree(f compose chroot.fileSystem[S](prefix)))
      ()
    }).run

  def runT(run: Run): FileSystemErrT[F, ?] ~> FsTask =
    Hoist[FileSystemErrT].hoist(run)

  def runLog[A](run: Run, p: Process[F, A]): Task[IndexedSeq[A]] =
    p.translate[Task](run).runLog

  def runLogT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[IndexedSeq[A]] =
    p.translate[FsTask](runT(run)).runLog

  def execT[A](run: Run, p: Process[FileSystemErrT[F, ?], A]): FsTask[Unit] =
    p.translate[FsTask](runT(run)).run

  ////

  implicit class FSExample(s: String) {
    def >>*[A: AsResult](fa: => F[A])(implicit run: Run): Example =
      s >> run(fa).run
  }

  implicit class RunFsTask[A](fst: FsTask[A]) {
    import Leibniz.===

    def runEither: Either[FileSystemError, A] =
      fst.run.run.toEither

    def runOption(implicit ev: A === Unit): Option[FileSystemError] =
      fst.run.run.swap.toOption

    def runVoid(implicit ev: A === Unit): Unit =
      fst.run.void.run
  }
}

object FileSystemTest {
  val oneDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("a" -> Data.Int(1))))

  val anotherDoc: Vector[Data] =
    Vector(Data.Obj(ListMap("b" -> Data.Int(2))))

  def manyDocs(n: Int): EStream[Data] =
    EStream.range(0, n) map (n => Data.Obj(ListMap("a" -> Data.Int(n))))

  def vectorFirst[A]: Optional[Vector[A], A] =
    Index.index[Vector[A], Int, A](0)

  //--- FileSystems to Test ---

  /** FileSystem Under Test */
  final case class FileSystemUT[S[_]](name: String, f: S ~> Task, testPrefix: AbsDir[Sandboxed])

  def allFsUT: Task[NonEmptyList[FileSystemUT[FileSystem]]] =
    (inMemUT |@| externalFileSystems)(_ <:: _)

  def externalFileSystems: Task[NonEmptyList[FileSystemUT[FileSystem]]] = {
    def fileSystemNamed(n: String, p: AbsDir[Sandboxed])
                       : OptionT[Task, FileSystemUT[FileSystem]] = {
      TestConfig.loadConfig(n) flatMapF {
        case MongoDbConfig(cs) => mongoDbUT(n, cs, p)
        case other             => Task.fail(new RuntimeException(s"Unsupported filesystem config: $other"))
      }
    }

    def noBackendsFound: Throwable = new RuntimeException(
      "No external backends to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.backendEnvName).mkString(", ")
    )


    TestConfig.testDataPrefix flatMap { prefix =>
      TestConfig.backendNames
        .traverse(n => fileSystemNamed(n, prefix).run)
        .flatMap(_.flatten.toNel.cata(Task.now, Task.fail(noBackendsFound)))
    }
  }

  private val inMemUT = {
    lazy val f = InMem.run
    Task.delay(FileSystemUT("in-memory", f, rootDir))
  }

  private def InMem: Task[FileSystem ~> Task] =
    inmemory.runStatefully map { f =>
      f compose interpretFileSystem(
                  inmemory.readFile,
                  inmemory.writeFile,
                  inmemory.manageFile)
    }

  private def mongoDbUT(name: String, cs: ConnectionString, prefix: AbsDir[Sandboxed])
                       : Task[FileSystemUT[FileSystem]] = {
    val defaultDb = flatten(none, none, none, _.some, κ(none), prefix)
                      .unite.headOption getOrElse "quasar-test"

    lazy val f = mongoFs(cs, DefaultDb(defaultDb)).run

    pathSalt map (s => FileSystemUT(name, f, prefix </> dir(s)))
  }

  private def mongoFs(cs: ConnectionString, defDb: DefaultDb): Task[FileSystem ~> Task] =
    for {
      client <- Task.delay(MongoClients create cs)
      rfile  <- mongofs.readfile.run(client)
      wfile  <- mongofs.writefile.run(client)
      mfile  <- mongofs.managefile.run(client, defDb)
    } yield {
      interpretFileSystem(
        rfile compose mongofs.readfile.interpret,
        wfile compose mongofs.writefile.interpret,
        mfile compose mongofs.managefile.interpret)
    }

  private def pathSalt: Task[String] =
    Task.delay(scala.util.Random.nextInt().toHexString)
}
