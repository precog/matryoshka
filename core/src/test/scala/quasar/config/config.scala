package quasar.config

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => EnginePath}

import pathy._, Path._
import scalaz._, concurrent.Task, Scalaz._
import scala.util.Properties
import org.specs2.mutable._
import org.specs2.scalaz._

class ConfigSpec extends Specification with DisjunctionMatchers {
  import FsPath._

  sequential

  def printPosix[T](fp: FsPath[T, Sandboxed]) = printFsPath(posixCodec, fp)

  val TestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      EnginePath.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")))

  val BrokenTestConfig = Config(
    server = SDServerConfig(Some(92)),
    mountings = Map(
      EnginePath.Root -> MongoDbConfig("mongodb://slamengine:slamengine@ds045088.mongolab.com:45089/slamengine-test-01")))

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

  val OldConfigStr =
    """{
      |  "server": {
      |    "port": 92
      |  },
      |  "mountings": {
      |    "/": {
      |      "mongodb": {
      |        "database": "slamengine-test-01",
      |        "connectionUri": "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01"
      |      }
      |    }
      |  }
      |}""".stripMargin

  val ConfigStr =
    """{
      |  "server": {
      |    "port": 92
      |  },
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
      Config.fromString(ConfigStr) must beRightDisjunction(TestConfig)
    }

    "parse previous config" in {
      Config.fromString(OldConfigStr) must beRightDisjunction(TestConfig)
    }
  }

  "toString" should {
    "render same config" in {
      Config.toString(TestConfig) must_== ConfigStr
    }
  }

  // NB: Not possible to test windows deterministically at this point as cannot
  //     programatically set environment variables like we can with properties.
  "defaultPath" should {
    val comp = "SlamData/quasar-config.json"
    val macp = "Library/Application Support"
    val posixp = ".config"

    "mac when home dir" in {
      val p = withProp("user.home", "/home/foo", Config.defaultPathForOS(file("quasar-config.json"))(OS.mac))
      printPosix(p.run) ==== s"/home/foo/$macp/$comp"
    }

    "mac no home dir" in {
      val p = withoutProp("user.home", Config.defaultPathForOS(file("quasar-config.json"))(OS.mac))
      printPosix(p.run) ==== s"./$macp/$comp"
    }

    "posix when home dir" in {
      val p = withProp("user.home", "/home/bar", Config.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
    }

    "posix when home dir with trailing slash" in {
      val p = withProp("user.home", "/home/bar/", Config.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"/home/bar/$posixp/$comp"
    }

    "posix no home dir" in {
      val p = withoutProp("user.home", Config.defaultPathForOS(file("quasar-config.json"))(OS.posix))
      printPosix(p.run) ==== s"./$posixp/$comp"
    }
  }

  "toFile" should {
    "create loadable config" in {
      withTestConfigFile(fp =>
        Config.toFile(TestConfig, Some(fp)) *>
        Config.fromFile(fp).run
      ).run must beRightDisjunction(TestConfig)
    }
  }

  "fromFileOrEmpty" should {
    "result in empty config when file not found" in {
      withTestConfigFile(fp =>
        Config.fromFileOrEmpty(Some(fp)).run
      ).run must beRightDisjunction(Config.empty)
    }
  }

  "loadAndTest" should {
    "load a correct config" in {
      withTestConfigFile(fp =>
        Config.toFile(TestConfig, Some(fp)) *>
        Config.loadAndTest(fp).run
      ).run must beRightDisjunction(TestConfig)
    }

    "fail on a config with incorrect mounting" in {
      withTestConfigFile(fp =>
        Config.toFile(BrokenTestConfig, Some(fp)) *>
        Config.loadAndTest(fp).run
      ).run must beLeftDisjunction
    }
  }

  "MongoDbConfig.UriPattern" should {
    import MongoDbConfig._

    "parse simple URI" in {
      "mongodb://localhost" match {
        case ParsedUri(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse simple URI with trailing slash" in {
      "mongodb://localhost/" match {
        case ParsedUri(_, _, host, _, _, _, _) => host must_== "localhost"
      }
    }

    "parse user/password" in {
      "mongodb://me:pwd@localhost" match {
        case ParsedUri(user, pwd, _, _, _, _, _) =>
          user must beSome("me")
          pwd must beSome("pwd")
      }
    }

    "parse port" in {
      "mongodb://localhost:5555" match {
        case ParsedUri(_, _, _, port, _, _, _) => port must beSome(5555)
      }
    }

    "parse database" in {
      "mongodb://localhost/test" match {
        case ParsedUri(_, _, _, _, _, db, _) => db must beSome("test")
      }
    }

    "parse additional host(s)" in {
      "mongodb://host1,host2,host3:5555" match {
        case ParsedUri(_, _, _, _, hosts, _, _) => hosts must beSome(",host2,host3:5555")
      }
    }

    "parse options" in {
      "mongodb://host/?option1=foo&option2=bar" match {
        case ParsedUri(_, _, _, _, _, _, options) => options must beSome("option1=foo&option2=bar")
      }
    }

    "parse all of the above" in {
      "mongodb://me:pwd@host1:5555,host2:5559,host3/test?option1=foo&option2=bar" match {
        case ParsedUri(user, pwd, host, port, extraHosts, db, options) =>
          user must beSome("me")
          pwd must beSome("pwd")
          host must_== "host1"
          port must beSome(5555)
          extraHosts must beSome(",host2:5559,host3")
          db must beSome("test")
          options must beSome("option1=foo&option2=bar")
      }
    }
  }
}
