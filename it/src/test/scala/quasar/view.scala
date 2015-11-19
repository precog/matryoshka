package quasar.physical.mongodb

import quasar.Predef._
import quasar.fp._
import quasar.specs2.SkippedOnUserEnv
import quasar._
import quasar.fs._

import org.specs2.execute.{Result, SkipException}
import org.specs2.scalaz.DisjunctionMatchers
import scalaz.stream._

// One way to test the view implementation: put the ViewBackend to be tested
// in front of each Backend supplied by the integration test setup. This is a
// bit of a bad fit because there is really no need to test views against
// _every_ backend, but this is simpler and requires less tedious stubbing
// than implementing some kind of mock Backend.
class ViewSpecs extends BackendTest with DisjunctionMatchers with SkippedOnUserEnv {
  import Backend._
  import Path.PathError._
  import Evaluator.EvalPathError

  def parse(query: String) =
    new sql.SQLParser().parse(sql.Query(query))
      .valueOr(e => scala.sys.error("bad query: " + e))

  backendShould(interactive.zips.run) { (prefix, backend, backendName, files) =>
    val relPrefix = prefix.asRelative
    val TestDir = relPrefix ++ testRootDir ++ genTempDir.run
    val ZipsPath = Path("/mnt/") ++ files.head

    val nested = NestedBackend(Map(DirNode("mnt") -> backend))

    val views = Map(
      Path("/view/simpleZips") -> config.ViewConfig(parse("select _id as zip, city, state from  \"/" + ZipsPath.simplePathname + "\"")),
      Path("/view/smallCities") -> config.ViewConfig(parse("select city as City, state as St, sum(pop) as Size from \"/" + ZipsPath.simplePathname + "\" group by city, state having sum(pop) <= 1000 order by city, state")),
      // NB: this view refers to the previous view using a relative path
      Path("/view/smallCityCounts") -> config.ViewConfig(parse("select St, count(*) from smallCities group by St order by count(*) desc, St")),
      Path("/view/badRef") -> config.ViewConfig(parse("""select foo from "/mnt/test/nonexistent"""")))

    val root = ViewBackend(nested, views)

    "identify view as a mount" in {
      root.ls(Path("/view/")).run.run.fold[Result](
        e => failure(e.toString),
        _ must contain(FilesystemNode(Path("smallCities"), Mount)))
    }

    "include view ancestors as plain directories" in {
      root.ls(Path.Root).run.run.fold[Result](
        e => failure(e.toString),
        _ must contain(FilesystemNode(Path("view/"), Plain)))
    }

    "count non-view" in {
      root.count(ZipsPath).run.run must beRightDisjunction(29353)
    }

    "count view" in {
      root.count(Path("/view/smallCities")).run.run must beRightDisjunction(7809)
    }

    "count view with view reference" in {
      root.count(Path("/view/smallCityCounts")).run.run must beRightDisjunction(51)
    }

    "count view with bad reference" in {
      root.count(Path("/view/badRef")).run.run must beLeftDisjunction(
        PEvalError(EvalPathError(NonexistentPathError(Path("/test/nonexistent"), None))))
    }

    "scan non-view" in {
      root.scan(ZipsPath, 0, Some(10)).map(κ(1)).sum.runLast.run.run must
        beRightDisjunction(Some(10))
    }

    "scan view" in {
      root.scan(Path("/view/smallCities"), 10, Some(10)).runLog.run.run.fold[Result](
        e => failure(e.toString),
        _ must contain(
          Data.Obj(ListMap(
            "City" -> Data.Str("ACME"),
            "St" -> Data.Str("WA"),
            "Size" -> Data.Int(471))),
          Data.Obj(ListMap(
            "City" -> Data.Str("ADAMS"),
            "St" -> Data.Str("ND"),
            "Size" -> Data.Int(312)))))
    }

    "scan view with view reference" in {
      root.scan(Path("/view/smallCityCounts"), 0, Some(1)).runLog.run.run must
        beRightDisjunction(Vector(
          Data.Obj(ListMap(
            "St" -> Data.Str("IA"),
            "1" -> Data.Int(428)))))
    }

    "scan view with bad reference" in {
      root.scan(Path("/view/badRef"), 0, None).runLog.run.run must beLeftDisjunction(
        ResultProcessingError(PEvalError(EvalPathError(NonexistentPathError(Path("/test/nonexistent"), None)))))
    }

    "trivial query referring to a view" in {
      val query = """select * from "/view/smallCities""""
      root.evalResults(QueryRequest(parse(query), None, Variables(Map.empty))).fold[Result](
        e => failure(e.toString),
        _.take(1).runLog.run.run must beRightDisjunction(Vector(Data.Obj(ListMap(
          "City" -> Data.Str("AARON"),
          "St" -> Data.Str("KY"),
          "Size" -> Data.Int(270)))))
      )
    }

    "less-trivial query referring to a view" in {
      // Refers to the shape created in the view query
      val query = """select City || ', ' || St from "/view/smallCities" where Size < 500"""
      root.evalResults(QueryRequest(parse(query), None, Variables(Map.empty))).fold[Result](
        e => failure(e.toString),
        _.take(1).runLog.run.run must beRightDisjunction(Vector(Data.Obj(ListMap(
          "0" -> Data.Str("AARON, KY")))))
      )
    }

    "query with view referencing a view" in {
      val query = """select max("1") from "/view/smallCityCounts""""
      root.evalResults(QueryRequest(parse(query), None, Variables(Map.empty))).fold[Result](
        e => failure(e.toString),
        _.runLog.run.run must beRightDisjunction(Vector(Data.Obj(ListMap(
          "0" -> Data.Int(428))))))
    }

    "query with un-renamed projection" in {
      // NB: the composed query ends up with references to city, state, and zip in
      // both the source table and the subquery result.
      val query = """select zip from "/view/simpleZips" where city = 'BOULDER' and state = 'CO' order by zip"""
      root.evalResults(QueryRequest(parse(query), None, Variables(Map.empty))).fold[Result](
        e => failure(e.toString),
        _.runLog.run.run must beRightDisjunction(Vector(
          Data.Obj(ListMap("zip" -> Data.Str("80301"))),
          Data.Obj(ListMap("zip" -> Data.Str("80302"))),
          Data.Obj(ListMap("zip" -> Data.Str("80303"))),
          Data.Obj(ListMap("zip" -> Data.Str("80304"))))))
    }

    "query with view with bad reference" in {
      val query = """select * from "/view/badRef""""
      root.evalResults(QueryRequest(parse(query), None, Variables(Map.empty))).fold[Result](
        e => failure(e.toString),
        _.runLog.run.run must beLeftDisjunction(
          PEvalError(EvalPathError(NonexistentPathError(Path("/test/nonexistent"), None)))))
    }

    val oneDoc = Process.emit(Data.Obj(ListMap("a" -> Data.Int(0))))

    "save to view: an error" in {
      root.save(Path("/view/smallCities"), oneDoc).run.run must
        beLeftDisjunction(ProcessingError.ViewWriteError(Path("view/smallCities")))
    }

    "move from view path: an error" in {
      root.move(Path("/view/smallCities"), Path("/foo/bar"), Overwrite).run.run must
        beLeftDisjunction(PathTypeError(Path("view/smallCities"), Some("cannot move view")))
    }

    "move to view path: an error" in {
      root.move(Path("/foo/bar"), Path("/view/smallCities"), Overwrite).run.run must
        beLeftDisjunction(PathTypeError(Path("view/smallCities"), Some("cannot move file to view location")))
    }

    "append to view: an error" in {
      root.append(Path("/view/smallCities"), oneDoc).runLog.run.run must
        beLeftDisjunction(PathTypeError(Path("view/smallCities"), Some("cannot write to view")))
    }

    "delete view: an error" in {
      root.delete(Path("/view/smallCities")).run.run must
        beLeftDisjunction(PathTypeError(Path("view/smallCities"), Some("cannot delete view")))
    }

    val cleanup = step {
      deleteTempFiles(backend, TestDir).run
    }
  }
}
