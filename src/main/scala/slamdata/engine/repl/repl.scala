package slamdata.engine

import java.io.IOException
import org.jboss.aesh.console.Console
import org.jboss.aesh.console.AeshConsoleCallback
import org.jboss.aesh.console.ConsoleOperation
import org.jboss.aesh.console.Prompt
import org.jboss.aesh.console.settings.SettingsBuilder

import slamdata.engine.std._
import slamdata.engine.sql._
import slamdata.engine.analysis._

import scalaz.{NonEmptyList, Show}
import scalaz.std.string._
import scalaz.syntax._

object Repl {
  def main(args: Array[String]) {
    val console = new Console(new SettingsBuilder().parseOperators(false).create())

    console.setPrompt(new Prompt("slamdata$ "))
    console.setConsoleCallback(new AeshConsoleCallback() {
      override def execute(output: ConsoleOperation): Int = {
        if (output.getBuffer == "exit") {
          try {
            console.stop()
          } catch {
            case e: IOException => e.printStackTrace()
          }
        } else {
          val out = console.getShell.out()

          implicit val arrow = AnalysisArrow[Node, NonEmptyList[SemanticError]]

          new SQLParser().parse(output.getBuffer).fold(
            error => out.println("SQL could not be parsed:\n" + error),
            select => {
              import SemanticAnalysis._

              out.println("Successfully parsed SQL: \n" + select.sql)

              val phases = arrow.compose(FunctionBind[Provenance](StdLib), arrow.compose(ProvenanceInfer, ScopeTables[Unit]))

              phases(tree(select)).fold(
                error => out.println(Show[NonEmptyList[SemanticError]].show(error).toString),
                success => {

                }
              )
            }
          )
        }

        0
      }
    })
    console.start()
  }
}