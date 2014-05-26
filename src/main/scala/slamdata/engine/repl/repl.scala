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
import slamdata.engine.analysis.fixplate._
import slamdata.engine.physical.mongodb._

import scalaz.concurrent.{Node => _, _}
import scalaz.{NonEmptyList, Show}
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.std.map._
import scalaz.std.option._
import scalaz.syntax._

import slamdata.engine.physical.mongodb.util
import slamdata.engine.config._

object Repl {
  def main(args: Array[String]) {
    val console = new Console(new SettingsBuilder().parseOperators(false).create())

    val config = args.headOption.map(Config.fromFile _).getOrElse(
      Task.now(
        Config(
          mountings = Map(
            "/" -> MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
          )
        )
      )
    )

    val mongoConfig = config.map(_.mountings.get("/") match {
      case Some(config : MongoDbConfig) => config
      case _ => throw new RuntimeException("Bogus configuration, expected / with MongoDB config")
    })

    val database = mongoConfig.flatMap(util.createMongoDB(_))

    val evaluator = database.map(MongoDbEvaluator(_))

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

          new SQLParser().parse(output.getBuffer).fold(
            error => out.println("SQL could not be parsed:\n" + error),
            select => {
              import SemanticAnalysis._

              out.println("Successfully parsed SQL: \n" + select.sql)

              try {
                AllPhases(tree(select)).fold(
                  error => out.println(Show[NonEmptyList[SemanticError]].show(error).toString),
                  tree => {
                    println("Successfully attributed SQL AST")

                    println(Show[AnnotatedTree[Node, ((Type, Option[Func]), Provenance)]].show(tree).toString)

                    println("Beginning compilation to logical plan")

                    Compiler.compile(tree).fold(
                      error => out.println(error),
                      plan  => {
                        out.println(Show[Term[LogicalPlan]].show(plan).toString)

                        MongoDbPlanner.plan(plan).fold(
                          error => out.println(error),
                          plan  => {
                            out.println(plan)

                            (for {
                              db   <- database
                              col  <- Task.delay(db.getCollection("slamengine_tmp_output"))
                              _    <- Task.delay(col.drop())
                              eval <- evaluator
                              _    <- eval.execute(plan, "slamengine_tmp_output")
                              _    <- Task.delay {
                                        val output = col.find().limit(10).toArray.toArray.mkString("\n") + "\n..."

                                        println(output)
                                      }
                            } yield Unit).run
                          }
                        )
                      }
                    )
                  }
                )
              } catch {
                case e: Throwable => e.printStackTrace
              }
            }
          )
        }

        0
      }
    })
    console.start()
  }
}