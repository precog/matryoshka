package quasar

import Predef._
import Backend.ProcessingTask
import fs.Path
import sql.{Query, SQLParser}

import scalaz.Scalaz
import scalaz.concurrent.Task
import scalaz.stream.Process
import Scalaz._

/**
 * Convenience methods that trade strong type safety for simplicity.
 * Intended to be used in test code where a failure is unexpected and may be useful in
 * a scripting or console context for playing around with the Quasar API.
 */
package object interactive {
  /**
   * Execute an SQL query on the backend and store the result in the provided `destinationPath`
   * @param backend The Backend on which to run this query.
   * @param query The SQL query to run
   * @param destinationPath The path at which to store the result of the query.
   * @return The result path where the result of this query was stored. For now, this should be equal to the
   *         `destinationPath` specified as an argument, but that might eventually change. All failures are encoded as
   *         a `Task` failure for convenience at the expense of safety and strong typing. The failure will be a
   *         `scala.Exception` that contains a string message describing the nature of the failure.
   */
  def run(backend: Backend, query: String, destinationPath: String): Task[ResultPath] = {
    val parser = new SQLParser()
    for {
      sql <- Task.fromDisjunction(parser.parse(Query(query)).leftMap(parseError => new scala.Exception("Parse error" + parseError.message)))
      query = QueryRequest(sql, Some(Path(destinationPath)), Variables(Map()))
      resultPath <- backend.run(query).fold(
        err => Task.fail(new scala.Exception(err.message)),
        a => a.run.flatMap(either => Task.fromDisjunction(either.leftMap(e => new scala.Exception("Evaluation error" + e.message))))
      )._2
    } yield resultPath
  }

  /**
   * Execute an SQL query on the backend and return the result as a stream.
   * @param backend The `Backend` on which to run this query.
   * @param query The SQL query to evaluate
   * @return A Stream of `Data` representing the result of the query.
   */
  def eval(backend: Backend, query: String): Process[ProcessingTask, Data] = {
    val parser = new SQLParser()
    parser.parse(Query(query)).fold(
      err => Process.fail(new scala.Exception("Parser Error" + err.message)),
      sql => backend.eval(QueryRequest(sql, None, Variables(Map()))).run._2.fold(
        err => Process.fail(new scala.Exception("Compilation Error" + err.message)),
        process => process
      )
    )
  }
}
