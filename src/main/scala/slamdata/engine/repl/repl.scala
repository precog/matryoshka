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
    val ExitPattern         = "(?:exit)|(?:quit)".r
    val CdPattern           = "cd +(.+)".r
    val SelectPattern       = "(select +.+)".r
    val NamedSelectPattern  = "(\\w+) *:= *(select +.+)".r
    val LsPattern           = "ls( +.+)".r
    val HelpPattern         = "(?:help)|(?:commands)".r

    case object Exit extends Command
    case object Unknown extends Command
    case object Help extends Command
    case class Cd(dir: String) extends Command
    case class Select(name: Option[String], query: String) extends Command
    case class Ls(dir: Option[String]) extends Command
  }

  private type Printer = String => Task[Unit]

  case class RunState(printer: Printer, mounted: Map[String, Backend], path: String = "/", unhandled: Option[Command] = None)

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
      case SelectPattern(query) => Select(None, query)
      case NamedSelectPattern(name, query) => Select(Some(name), query)
      case LsPattern(path)      => Ls(if (path.trim.length == 0) None else Some(path.trim))
      case HelpPattern()        => Help
      case _                    => Unknown
    })
  }

  def showHelp(state: RunState): Process[Task, Unit] = Process.eval(
    state.printer(
      """|SlamEngine REPL, Copyright (C) 2014 SlamData Inc.
         |
         | Available commands:
         |   exit
         |   help
         |   cd [path]
         |   select [query]
         |   [id] := select [query]
         |   ls [path]""".stripMargin
    )
  )

  def showError(state: RunState): Process[Task, Unit] = Process.eval(
    state.printer(
      """|Unrecognized command!""".stripMargin
    )
  )

  def select(state: RunState, query: String, name: Option[String]): Process[Task, Unit] = state.mounted.get(state.path).map { backend =>
    import state.printer

    Process.eval(backend.execute(query, name.getOrElse("tmp")).flatMap {
      case (log, results) =>
        for {
          _ <- printer(log.toString)

          val preview = (results |> process1.take(10)).runLog.run

          _ <- if (preview.length == 0) printer("No results found")
               else printer(preview.mkString("\n") + "\n...\n")
        } yield ()
    }) handle {
      case _ => Process.eval(printer("An error occurred during evaluation of the query"))
    }
  }.getOrElse(Process.eval(state.printer("There is no database mounted to the path " + state.path)))

  def ls(state: RunState, path: Option[String]): Process[Task, Unit] = Process.eval(
    state.printer("Sorry, no information on directory structure yet.")
  )

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
        (commands |> process1.scan(RunState(printer, mounted)) {
          case (state, input) =>
            input match {
              case Cd(path) => state.copy(path = path, unhandled = None)
              case x        => state.copy(unhandled = Some(x))
            }
        }) flatMap {
          case s @ RunState(_, _, path, Some(command)) => command match {
            case Exit           => throw Process.End
            case Help           => showHelp(s)
            case Select(n, q)   => select(s, q, n)
            case Ls(dir)        => ls(s, dir)

            case _ => showError(s)
          }

          case _ => Process.eval(Task.now(()))
        }
    ).join
  }

  def main(args: Array[String]) {
    (for {
      _ <- run(args).run
      _ <- Task.delay(System.exit(0))
    } yield ()).run
  }
}