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
import scalaz.{Node => _, _}
import Scalaz._
import scalaz.stream._

import slamdata.engine.physical.mongodb.util
import slamdata.engine.config._

import scala.util.matching._

object Repl {
  val DefaultConfig = Config(
    mountings = Map(
      "/" -> MongoDbConfig("slamengine-test-01", "mongodb://slamengine:slamengine@ds045089.mongolab.com:45089/slamengine-test-01")
    )
  )

  sealed trait Command
  object Command {
    val ExitPattern   = "(?:exit)|(?:quit)".r
    val CdPattern     = "cd +(.+)".r
    val SelectPattern = "(select +.+)".r
    val LsPattern     = "ls( +.+)".r
    val HelpPattern   = "(?:help)|(?:commands)".r

    case object Exit extends Command
    case object Unknown extends Command
    case object Help extends Command
    case class Cd(dir: String) extends Command
    case class Select(query: String) extends Command
    case class Ls(dir: Option[String]) extends Command
  }

  private type Printer = String => Task[Unit]

  private def commandInput: Task[(Printer, Process[Task, Command])] = Task.delay {
    import Command._
      
    val console = new Console(new SettingsBuilder().parseOperators(false).create())

    console.setPrompt(new Prompt("slamdata$ "))

    val out = (s: String) => Task.delay(console.getShell.out().println(s))

    val (queue, source) = async.queue[String]
    
    console.setConsoleCallback(new AeshConsoleCallback() {
      override def execute(output: ConsoleOperation): Int = {
        val input = output.getBuffer.trim

        queue.enqueue(input)

        0
      }
    })

    console.start()

    (out, source.map {
      case ExitPattern()        => Exit
      case CdPattern(path)      => Cd(path)
      case SelectPattern(query) => Select(query)
      case LsPattern(path)      => Ls(if (path.trim.length == 0) None else Some(path.trim))
      case HelpPattern()        => Help
      case _                    => Unknown
    })
  }

  def showHelp(printer: Printer): Process[Task, Unit] = Process.eval(
    printer(
      """|SlamEngine REPL, Copyright (C) 2014 SlamData Inc.
         |
         | Available commands:
         |   exit
         |   help
         |   cd [path]
         |   select [query]
         |   ls [path]""".stripMargin
    )
  )

  def showError(printer: Printer): Process[Task, Unit] = Process.eval(
    printer(
      """|Unrecognized command!""".stripMargin
    )
  )

  def select(printer: Printer, mounted: Map[String, Backend], path: String, query: String): Process[Task, Unit] = Process.eval(Task.delay {
    println("Selecting query: " + query + " in path " + path)
  })

  def ls(printer: Printer, path: Option[String]): Process[Task, Unit] = Process.eval(
    printer("Sorry, no information on directory structure yet.")
  )

  case class RunState(path: String = "/", unhandled: Option[Command] = None)

  def run(args: Array[String]): Process[Task, Unit] = {
    import Command._

    val mounted = for {
      config  <- args.headOption.map(Config.fromFile _).getOrElse(Task.now(DefaultConfig))
      mounted <- Mounter.mount(config)
    } yield mounted

    Process.eval(for {
        tuple <- commandInput

        val (printer, commands) = tuple

        mounted <- mounted 
      } yield 
        (commands |> process1.scan(RunState()) {
          case (state, input) =>
            input match {
              case Cd(path) => RunState(path = path, unhandled = None)
              case x        => RunState(unhandled = Some(x))
            }
        }) flatMap {
          case RunState(path, Some(command)) => command match {
            case Exit           => throw Process.End
            case Help           => showHelp(printer)
            case Select(query)  => select(printer, mounted, path, query)
            case Ls(dir)        => ls(printer, dir)

            case _ => showError(printer)
          }

          case _ => Process.eval(Task.now(Unit: Unit))
        }
    ).join
  }

  def main(args: Array[String]) {
    (for {
      _ <- run(args).run
      _ <- Task.delay(System.exit(0))
    } yield Unit).run
  }
}