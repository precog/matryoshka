package quasar.config

import com.mongodb.ConnectionString
import quasar.Predef._
import quasar.config.FsPath.NonexistentFileError
import quasar.Evaluator.EnvironmentError.EnvFsPathError
import quasar.fp._
import quasar.fs.{Path => QPath}

import argonaut._, Argonaut._
import pathy._, Path._
import scalaz._, concurrent.Task, Scalaz._
import scala.util.Properties
import org.specs2.mutable._
import org.specs2.scalaz._
import org.specs2.ScalaCheck
import org.scalacheck._

abstract class ConfigSpec[Config: CodecJson] extends Specification with DisjunctionMatchers {
  import FsPath._

  sequential

  def configOps: ConfigOps[Config]

  def sampleConfig(uri: ConnectionString): Config

  def printPosix[T](fp: FsPath[T, Sandboxed]) = printFsPath(posixCodec, fp)

  val host = "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089"
  val invalidHost = host.replace('9', '8')
  val dbName = "slamengine-test-01"
  val validURI = new ConnectionString(s"$host/$dbName")
  val invalidURI = new ConnectionString(s"$invalidHost/$dbName")
  val TestConfig = sampleConfig(validURI)

  val BrokenTestConfig = sampleConfig(invalidURI)

  def testConfigFile: Task[FsPath.Aux[Rel, File, Sandboxed]] =
    Task.delay(scala.util.Random.nextInt.toString)
      .map(i => Uniform(currentDir </> file(s"test-config-${i}.json")))

  def withTestConfigFile[A](f: FsPath.Aux[Rel, File, Sandboxed] => Task[A]): Task[A] = {
    import java.nio.file._

    def deleteIfExists(fp: FsPath[File, Sandboxed]): Task[Unit] =
      systemCodec
        .map(c => printFsPath(c, fp))
        .flatMap(s => Task.delay(Files.deleteIfExists(Paths.get(s))))
        .void

    testConfigFile >>= (fp => f(fp) onFinish κ(deleteIfExists(fp)))
  }

  def getProp(n: String): Task[Option[String]] =
    Task.delay(Properties.propOrNone(n))

  def setProp(n: String, v: String): Task[Unit] =
    Task.delay(Properties.setProp(n, v)).void

  def clearProp(n: String): Task[Unit] =
    Task.delay(Properties.clearProp(n)).void

  def withProp[A](n: String, v: String, t: => Task[A]): Task[A] =
    for {
      prev <- getProp(n)
      _    <- setProp(n, v)
      a    <- t onFinish κ(prev.cata(setProp(n, _), Task.now(())))
    } yield a

  def withoutProp[A](n: String, t: => Task[A]): Task[A] =
    for {
      prev <- getProp(n)
      _    <- clearProp(n)
      a    <- t onFinish κ(prev.cata(setProp(n, _), Task.now(())))
    } yield a

  val ConfigStr =
    """{
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "connectionUri": "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse valid config" in {
      configOps.fromString(ConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      configOps.showInstance.shows(TestConfig) must_== ConfigStr
    }
  }

  "View encoding" should {
    import quasar.sql
    import argonaut._, Argonaut._

    val read = sql.Select(sql.SelectAll, sql.Row(List(sql.Splice(None))), Some(sql.TableRelationAST("zips", None)), None, None, None)

    "encode" in {
      ViewConfig.Codec.encode(ViewConfig(read)) must_==
        Json("connectionUri" := "sql2:///?q=%28select+*+from+zips%29")
    }

    def decode(js: Json) = ViewConfig.Codec.decode(js.hcursor).result

    "decode" in {
      decode(Json("connectionUri" := "sql2:///?q=%28select+*+from+zips%29")) must beRightDisjunction(
        ViewConfig(read))
    }

    "decode with bad scheme" in {
      decode(Json("connectionUri" := "foo:///?q=%28select+*+from+zips%29")) must beLeftDisjunction
    }

    "decode with unparseable URI" in {
      decode(Json("connectionUri" := "?")) must beLeftDisjunction
    }

    "decode with bad encoding" in {
      decode(Json("connectionUri" := "sql2:///?q=%28select+*+from+zips%29%")) must beLeftDisjunction
    }
  }

  // NB: Not possible to test windows deterministically at this point as cannot
  //     programatically set environment variables like we can with properties.
  "defaultPath" should {
    val comp = "quasar-config.json"
    val macp = "Library/Application Support"
    val posixp = ".config"

    "mac when home dir" in {
      val p = withProp("user.home", "/home/foo", configOps.defaultPathForOS(file("quasar-config.json"))(OS.mac))
      printPosix(p.run) ==== s"/home/foo/$macp/$comp"
    }

    "mac no home dir" in {
      val p = withoutProp("user.home", configOps.defaultPathForOS(file("quasar-config.json"))(OS.mac))
      printPosix(p.run) ==== s"./$macp/$comp"
    }

    "posix when home dir" in {
      val p = withProp("user.home", "/home/bar", configOps.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
    }

    "posix when home dir with trailing slash" in {
      val p = withProp("user.home", "/home/bar/", configOps.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
    }

    "posix no home dir" in {
      val p = withoutProp("user.home", configOps.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"./$posixp/$comp"
    }
  }

  "toFile" should {
    "create loadable config" in {
      withTestConfigFile(fp =>
        configOps.toFile(TestConfig, Some(fp)) *>
        configOps.fromFile(fp).run
      ).run must beRightDisjunction(TestConfig)
    }
  }

  "fromFileOrDefaultPaths" should {
    "result in error when file not found" in {
      val (p, r) =
        withTestConfigFile(fp =>
          configOps.fromFileOrDefaultPaths(Some(fp)).run.map((fp, _))
        ).run
      r must beLeftDisjunction(EnvFsPathError(NonexistentFileError(p)))
    }
  }

  "loadAndTest" should {
    "load a correct config" in {
      withTestConfigFile(fp =>
        configOps.toFile(TestConfig, Some(fp)) *>
        configOps.loadAndTest(fp).run
      ).run must beRightDisjunction(TestConfig)
    }

    "fail on a config with incorrect mounting" in {
      withTestConfigFile(fp =>
        configOps.toFile(BrokenTestConfig, Some(fp)) *>
        configOps.loadAndTest(fp).run
      ).run must beLeftDisjunction
    }
  }
}

class CoreConfigSpec extends ConfigSpec[CoreConfig] with ScalaCheck {

  def configOps: ConfigOps[CoreConfig] = CoreConfig

  def sampleConfig(uri: ConnectionString): CoreConfig = CoreConfig(
    mountings = Map(
      QPath.Root -> MongoDbConfig(uri)))

  val OldConfigStr =
    """{
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "database": "slamengine-test-01",
      |        "connectionUri": "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"
      |      }
      |    }
      |  }
      |}""".stripMargin

  "fromString" should {
    "parse previous config" in {
      configOps.fromString(OldConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "encoding" should {
    import CoreConfigGen._

    "round-trip any well-formed config" ! prop { (cfg: CoreConfig) =>
      val json = CoreConfig.Codec.encode(cfg)
      val cfg2 = CoreConfig.Codec.decode(json.hcursor)
      cfg2.result must beRightDisjunction(cfg)
    }
  }
}

object CoreConfigGen {
  import quasar.sql
  import Arbitrary._
  import Gen._

  implicit val arbitraryConfig: Arbitrary[CoreConfig] = Arbitrary {
    for {
      mounts <- listOf(mountGen)
    } yield CoreConfig(Map(mounts: _*))
  }

  def mountGen: Gen[(QPath, MountConfig)] = for {
    path <- arbitrary[String]
    cfg  <- Gen.oneOf(mongoCfgGen, viewCfgGen)
  } yield (QPath(path), cfg)

  def mongoCfgGen =
    Gen.const(MongoDbConfig(new ConnectionString("mongodb://localhost/test")))

  val SimpleQuery = sql.Select(sql.SelectAll,
    sql.Row(List(sql.Splice(None))),
    Some(sql.TableRelationAST("foo", None)),
    None, None, None)

  def viewCfgGen = Gen.const(ViewConfig(SimpleQuery))
}
