package quasar

import quasar.fp.liftMT
import quasar.fs._
import quasar.regression._
import quasar.sql._

import pathy.Path._
import scalaz.{~>, Hoist}
import scalaz.stream.Process
import scalaz.std.vector._
import scalaz.syntax.monad._

class ResultFileQueryRegressionSpec
  extends QueryRegressionTest[FileSystemIO](QueryRegressionTest.externalFS) {

  val read = ReadFile.Ops[FileSystemIO]

  val suiteName = "ResultFile Queries"

  def queryResults(expr: Expr, vars: Variables) = {
    import qfTransforms._

    type M[A] = FileSystemErrT[F, A]

    val tmpPath =
      DataDir </> file("out")

    val hoistM: M ~> CompExecM =
      execToCompExec compose[M] Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

    for {
      tmpFile <- toCompExec(manage.tempFileNear(tmpPath)).liftM[Process]
      outFile <- query.executeQuery(expr, vars, tmpFile).liftM[Process]
      cleanup =  hoistM(manage.delete(tmpFile))
      data    <- read.scanAll(outFile)
                   .translate(hoistM)
                   .onComplete(Process.eval_(cleanup))
    } yield data
  }
}
