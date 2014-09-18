package slamdata.engine

import org.specs2.mutable._

import scalaz._, Scalaz._
import scalaz.concurrent._

import argonaut._

import slamdata.engine._
import slamdata.engine.config._
import slamdata.engine.fp._
import slamdata.engine.fs._

trait TestConfig { def config: BackendConfig }
object TestConfig {
  case object mongolabs extends TestConfig { def config = MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01") }
  // case object mongolocal extends TestConfig { def config = MongoDbConfig("test", "mongodb://localhost:27017") }

  def all: NonEmptyList[TestConfig] = NonEmptyList(mongolabs)

  implicit val TestConfigDecodeJson: DecodeJson[TestConfig] =
    DecodeJson(c =>
      c.as[String].flatMap(name => all.list.find(name == _.toString).fold[DecodeResult[TestConfig]](DecodeResult(-\/ ("unrecognized backend: " + name -> c.history)))((v: TestConfig) => DecodeResult(\/- (v))))
    )

  implicit def NonEmptyListDecodeJson[A: DecodeJson]: DecodeJson[NonEmptyList[A]] =
    DecodeJson(c =>
      c.as[List[A]].flatMap {
        case a :: as => DecodeResult( \/- (NonEmptyList.nel(a, as)))
        case Nil     => DecodeResult(-\/  ("empty list" -> c.history))
      }
    )
}

trait BackendTest extends Specification {
  sequential  // makes it easier to clean up
  args.report(showtimes=true)

  def backends: NonEmptyList[(TestConfig, Task[Backend])] = TestConfig.all.map(tc => tc -> BackendDefinitions.All(tc.config).getOrElse(Task.fail(new RuntimeException("missing backend: " + tc))))

  val genTempFile: Task[Path] = Task.delay {
    Path("gen_" + scala.util.Random.nextInt().toHexString)
  }

  val genTempDir: Task[Path] = genTempFile.map(_.asDir)
  
  def tests(f: (TestConfig, Backend) => Unit): Unit = for (t <- backends) f(t._1, t._2.run)
  
  def deleteTempFiles(fs: FileSystem, dir: Path) = {
    val deleteAll = for {
      files <- fs.ls(dir)
      rez <- files.map(f => fs.delete(dir ++ f).attempt).sequenceU
    } yield rez
    val (errs, _) = unzipDisj(deleteAll.run)
    if (!errs.isEmpty) println("temp files not deleted: " + errs.map(_.getMessage).mkString("\n"))
  }
}

trait JsonMatchers extends org.specs2.matcher.TraversableMatchers {
  import argonaut._
  import org.specs2.matcher._
  
  /** Match an expected result, ignoring the actual value's _id field. */
  def beResult(expected: RenderedJson): Matcher[RenderedJson] = beResult("_id" :: Nil, expected)

  def beResult(ignoreFields: List[String], expected: RenderedJson): Matcher[RenderedJson] = new Matcher[RenderedJson] {
    def apply[S <: RenderedJson](s: Expectable[S]) = {
      val v = s.value
      
      val matched = (Parse.parse(v.value) |@| Parse.parse(expected.value)) { case (v, expected) =>
        def drop(json: JsonObject, fields: List[String]): JsonObject = fields.foldLeft(json) { case (json, f) => json - f }
        v.obj.map(drop(_, ignoreFields)).getOrElse(v) == expected.obj.getOrElse(expected)
      }.getOrElse(false)

      result(matched, s"matches $expected", s"does not match $expected", s)
    }
  }
  
  def matchAllResults(expected: Json*): Matcher[Seq[RenderedJson]] = {
    val exp = expected.map(json => beResult(RenderedJson(json.toString)))
    contain[RenderedJson](exactly(exp: _*))
  }
  
  def matchAllResultsInOrder(expected: Json*): Matcher[Seq[RenderedJson]] = {
    val exp = expected.map(json => beResult(RenderedJson(json.toString)))
    contain[RenderedJson](exactly(exp: _*).inOrder)
  }
  
  def matchResults(ignoreOrder: Boolean, matchAll: Boolean, ignoreFields: List[String], expected: List[Json]): Matcher[Seq[RenderedJson]] = {
    val exp = expected.map(json => beResult(ignoreFields, RenderedJson(json.toString)))
    contain(
      (ignoreOrder, matchAll) match {
        case (true, true) => exactly(exp: _*)
        case (true, false) => allOf(exp: _*)
        case (false, true) => exactly(exp: _*).inOrder
        case (false, false) => allOf(exp: _*).inOrder
      })
  }
}