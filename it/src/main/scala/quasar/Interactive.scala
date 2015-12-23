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

package quasar

import java.io.File

import Predef._
import pathy.Path.FileName
import quasar.Backend.{FilesystemNode, PPathError, ProcessingTask}
import fs.Path
import quasar.Planner.CompilationError
import quasar.fs.Path.PathError
import sql.{Query, SQLParser}
import quasar.Errors._

import scala.io.Source
import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process
import Scalaz._

/** Convenience methods that trade strong type safety for simplicity.
 *  Intended to be used in test code where a failure is unexpected and may be useful in
 *  a scripting or console context for playing around with the Quasar API.
 */
package object interactive {

  def safeTaskToNormalTask[E:Show]: ETask[E,?] ~> Task = new (ETask[E,?] ~> Task) {
    def apply[A](safeTask: ETask[E,A]) = safeTask.fold(
      err => Task.fail(new scala.Exception(Show[E].shows(err))),
      a => Task.now(a)
    ).join
  }

  final case class DataSource(content: Process[Task, String], name: String)
  object DataSource {
    def fromFile(file: File): DataSource = {
      val content = scalaz.stream.io.linesR(Source.fromFile(file))
      val name = FileName(file.getName).dropExtension.value
      DataSource(content, name)
    }
  }

  def loadDataSourceFromResources(name: String): Task[DataSource] = Task.delay {
    val content = scalaz.stream.io.linesR(getClass.getResourceAsStream(s"/tests/$name.data"))
    DataSource(content, name)
  }
  val cities = loadDataSourceFromResources("cities")
  val days = loadDataSourceFromResources("days")
  val jobs_jobinfo = loadDataSourceFromResources("jobs_jobinfo")
  val nested = loadDataSourceFromResources("nested")
  val nested_foo = loadDataSourceFromResources("nested_foo")
  val objectids = loadDataSourceFromResources("objectids")
  val olympics = loadDataSourceFromResources("olympics")
  val slamengine_commits = loadDataSourceFromResources("slamengine_commits")
  val smallZips = loadDataSourceFromResources("smallZips")
  val unicode = loadDataSourceFromResources("unicode")
  val usa_factbook = loadDataSourceFromResources("usa_factbook")
  val user_comments = loadDataSourceFromResources("user_comments")
  val webapp = loadDataSourceFromResources("webapp")
  val zips = loadDataSourceFromResources("zips")

  /** Execute an SQL query on the backend and store the result in the provided `destinationPath`
   *  @param backend The Backend on which to run this query.
   *  @param query The SQL query to run
   *  @param destinationPath The path at which to store the result of the query.
   *  @return The result path where the result of this query was stored. For now, this should be equal to the
   *         `destinationPath` specified as an argument, but that might eventually change. All failures are encoded as
   *         a `Task` failure for convenience at the expense of safety and strong typing. The failure will be a
   *         `scala.Exception` that contains a string message describing the nature of the failure.
   */
  def run(backend: Backend, query: String, destinationPath: Path): Task[ResultPath] = {
    val parser = new SQLParser()
    for {
      sql <- Task.fromDisjunction(parser.parse(Query(query)).leftMap(parseError => new scala.Exception("Parse error" + parseError.message)))
      query = QueryRequest(sql, Variables(Map()))
      resultPath <- backend.run(query, destinationPath).fold(
        err => Task.fail(new scala.Exception(err.message)),
        a => a.run.flatMap(either => Task.fromDisjunction(either.leftMap(e => new scala.Exception("Evaluation error" + e.message))))
      )._2
    } yield resultPath
  }

  /** Execute an SQL query on the backend and return the result as a stream.
   *  @param backend The `Backend` on which to run this query.
   *  @param query The SQL query to evaluate
   *  @return A Stream of `Data` representing the result of the query.
   *  @throws scala.Exception if the query [[scala.Predef.String]] cannot be parsed
   */
  def eval(backend: Backend, query: String): Process[ProcessingTask, Data] = {
    evalLog(backend, query).run._2.fold(
      err => Process.fail(new scala.Exception("Compilation Error" + err.message)),
      process => process
    )
  }

  /** Execute an SQL query on the backend and return the result as a stream.
   *  @param backend The `Backend` on which to run this query.
   *  @param query The SQL query to evaluate
   *  @return A Stream of `Data` representing the result of the query along with the [[scala.Vector]] of `quasar.PhaseResult`
   *  @throws scala.Exception if the query [[scala.Predef.String]] cannot be parsed
   */
  def evalLog(backend: Backend, query: String): EitherT[(Vector[PhaseResult], ?), CompilationError, Process[ProcessingTask, Data]] = {
    val parser = new SQLParser()
    parser.parse(Query(query)).fold(
      err => throw new scala.Exception("Parser Error" + err.message),
      sql => backend.eval(QueryRequest(sql, Variables(Map())))
    )
  }

  def ls(backend: Backend, path: Path): Task[Set[FilesystemNode]] = {
    safeTaskToNormalTask[PathError].apply(backend.ls(path))
  }

  def delete(backend: Backend, path: Path): Task[Unit] = {
    safeTaskToNormalTask[PathError].apply(backend.delete(path))
  }

  /** Provides a temporary path to use in order to test something.
   *  The temporary collection stored at this path is guaranteed to be cleaned up
   *  when this function completes.
   *  @param prefix The path at which to create the temporary collection
   */
  def withTemp[A](backend: Backend, prefix: Path)(body: Path => A):A = {
    val tempName = "out0" // TODO: Consider a unique path
    val file = Path(tempName)
    val tempPath = prefix ++ file
    val result = body(file)
    delete(backend, tempPath).run
    result
  }

  /** Loads a collection of data into the provided backend if not already there
   *  @param backend The backend into which to load the data
   *  @param path The path of the collection (file) that will contain the loaded data.
   *  @param source Stream of Data to load into the backend
   */
  def loadData(backend: Backend, path: Path, source: Process[Task,String]): ProcessingTask[Unit] = {
    implicit val codec = DataCodec.Precise
    backend.exists(path).leftMap(PPathError(_)).flatMap { exists =>
      if (exists)
        ().point[ProcessingTask]
      else {
        val data = source.flatMap(DataCodec.parse(_).fold(
          err => Process.fail( new RuntimeException("error loading: " + err.message)),
          j => Process.eval(Task.now(j))
        ))
        for {
          _ <- liftE(Task.delay(println("loading: " + path.simplePathname)))
          _ <- backend.save(path, data)
          _ <- liftE(Task.delay(println("...done")))
        } yield ()
      }
    }
  }

  /** Loads a collection of data into the provided backend if not already there
   *  Same as `loadData` but the source description is used for the collection name.
   *  @param backend The backend into which to load the data
   *  @param prefix The path under which to store the collection materialized from the [[DataSource]]
   *  @param source source from which to extract data
   */
  def loadData(backend: Backend, prefix: Path, source: DataSource): ProcessingTask[Unit] =
    loadData(backend, prefix ++ Path(source.name), source.content)

  /** Loads a collection of data into the provided backend if not already there
   *  Uses the file name to choose the name of the collectin in which to put the resulting data
   *  @param backend The backend into which to load the data
   *  @param prefix The path under which to store the collection materialized from the data file
   *  @param file file from which to the load the data
   */
  def loadFile(backend: Backend, prefix: Path, file: File): ProcessingTask[Unit] =
    loadData(backend, prefix, DataSource.fromFile(file))
}
