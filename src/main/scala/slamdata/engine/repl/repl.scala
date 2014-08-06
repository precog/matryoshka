package slamdata.engine.repl

import java.io.IOException
import org.jboss.aesh.console.Console
import org.jboss.aesh.console.AeshConsoleCallback
import org.jboss.aesh.console.ConsoleOperation
import org.jboss.aesh.console.Prompt
import org.jboss.aesh.console.settings.SettingsBuilder

import slamdata.engine._
import slamdata.engine.fs._
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

import slamdata.java.JavaUtil

object Repl {
  sealed trait Command
  object Command {
    val ExitPattern         = "(?:exit)|(?:quit)".r
    val CdPattern           = "cd(?: +(.+))?".r
    val SelectPattern       = "(select +.+)".r
    val NamedSelectPattern  = "(\\w+) *:= *(select +.+)".r
    val LsPattern           = "ls(?: +(.+))?".r
    val HelpPattern         = "(?:help)|(?:commands)|\\?".r
    val DebugPattern        = "set debug *= *(0|1|2)".r

    case object Exit extends Command
    case object Unknown extends Command
    case object Help extends Command
    case class Cd(dir: String) extends Command
    case class Select(name: Option[String], query: String) extends Command
    case class Ls(dir: Option[String]) extends Command
    case class Debug(level: DebugLevel) extends Command
  }

  private type Printer = String => Task[Unit]

  sealed trait DebugLevel
  object DebugLevel {
    case object Silent extends DebugLevel
    case object Normal extends DebugLevel
    case object Verbose extends DebugLevel
    
    def fromInt(code: Int): Option[DebugLevel] = code match {
      case 0 => Some(Silent)
      case 1 => Some(Normal)
      case 2 => Some(Verbose)
      case _ => None
    }
  }

  case class RunState(printer: Printer, mounted: Map[Path, Backend], path: Path = Path.Root, unhandled: Option[Command] = None, debugLevel: DebugLevel = DebugLevel.Normal)

  private def parseCommand(input: String): Command = {
    import Command._
    
    input match {
      case ExitPattern()        => Exit
      case CdPattern(path)      => Cd(if (path == null || path.trim.length == 0) "/" else path.trim)
      case SelectPattern(query) => Select(None, query)
      case NamedSelectPattern(name, query) => Select(Some(name), query)
      case LsPattern(path)      => Ls(if (path == null || path.trim.length == 0) None else Some(path.trim))
      case HelpPattern()        => Help
      case DebugPattern(code)   => Debug(DebugLevel.fromInt(code.toInt).getOrElse(DebugLevel.Normal))
      case _                    => Unknown
    }
  }

  private def commandInput: Task[(Printer, Process[Task, Command])] = Task.delay {
    val console = new Console(new SettingsBuilder().parseOperators(false).create())

    console.setPrompt(new Prompt("slamdata$ "))

    val out = (s: String) => Task.delay(console.getShell.out().println(s))

    val (queue, source) = async.queue[Command](Strategy.Sequential)
    
    console.setConsoleCallback(new AeshConsoleCallback() {
      override def execute(output: ConsoleOperation): Int = {
        val input = output.getBuffer.trim

        val command = parseCommand(input)
        command match {
          case Command.Exit => console.stop()
          case _ => ()
        }

        queue.enqueue(command)
        
        0
      }
    })

    console.start()

    (out, source)
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
         |   ls [path]
         |   set debug = [level]""".stripMargin
    )
  )

  def showError(state: RunState): Process[Task, Unit] = Process.eval(
    state.printer(
      """|Unrecognized command!""".stripMargin
    )
  )

  def select(state: RunState, query: String, name: Option[String]): Process[Task, Unit] = {
    def summarize[A](max: Int)(lines: IndexedSeq[A]): String =
      if (lines.lengthCompare(0) <= 0) "No results found"
      else (Vector("Results") ++ lines.take(max) ++ (if (lines.lengthCompare(max) > 0) "..." :: Nil else Nil)).mkString("\n")

    state.mounted.get(state.path).map { backend =>
      import state.printer

      Process.eval(backend.eval(Query(query), Path(name getOrElse("tmp"))) flatMap {
        case (log, results) =>
          for {
            _ <- printer(state.debugLevel match {
                case DebugLevel.Silent  => "Debug disabled"
                case DebugLevel.Normal  => log.mkString("\n\n")
                case DebugLevel.Verbose => log.mkString("\n\n")  // TODO
              })

            preview = (results |> process1.take(10 + 1)).runLog.run

            _ <- printer(summarize(10)(preview))
            _ <- printer("")
          } yield ()
      }) handle {
        case e : slamdata.engine.Error => Process.eval {
          for {
            _ <- printer("A SlamData-specific error occurred during evaluation of the query")
            _ <- printer(e.fullMessage)
          } yield ()
        }

        case e => Process.eval {
          // An exception was thrown during evaluation; we cannot recover any logging that
          // might have been done, but at least we can capture the stack trace to aid 
          // debugging:
          for {
            _ <- printer("A generic error occurred during evaluation of the query")
            - <- printer(JavaUtil.stackTrace(e))
          } yield ()
        }
      }
    }.getOrElse(Process.eval(state.printer("There is no database mounted to the path " + state.path)))
  }


  
  def ls(state: RunState, path: Option[String]): Process[Task, Unit] = Process.eval({
    import state.printer

    state.mounted.get(state.path).map { backend =>
      // TODO: Support nesting
      backend.dataSource.ls.flatMap { paths =>
        state.printer(paths.mkString("\n"))
      }
    }.getOrElse(state.printer("Sorry, no information on directory structure yet."))
  })

  def showDebugLevel(state: RunState, level: DebugLevel): Process[Task, Unit] = Process.eval(
    state.printer(
      s"""|Set debug level: $level""".stripMargin
    )
  )

  def run(args: Array[String]): Process[Task, Unit] = {
    import Command._

    val mounted = for {
      config  <- args.headOption.map(Config.fromFile _).getOrElse(Task.now(Config.DefaultConfig))
      mounted <- Mounter.mount(config)
    } yield mounted

    Process.eval(for {
        tuple <- commandInput

        (printer, commands) = tuple

        mounted <- mounted
      } yield 
        (commands |> process1.scan(RunState(printer, mounted)) {
          case (state, input) =>
            input match {
              case Cd(path)     => state.copy(path = Path(path), unhandled = None)
              case Debug(level) => state.copy(debugLevel = level, unhandled = some(Debug(level)))
              case x            => state.copy(unhandled = Some(x))
            }
        }) flatMap {
          case s @ RunState(_, _, path, Some(command), _) => command match {
            case Exit           => throw Process.End
            case Help           => showHelp(s)
            case Select(n, q)   => select(s, q, n)
            case Ls(dir)        => ls(s, dir)
            case Debug(level)   => showDebugLevel(s, level)

            case _ => showError(s)
          }

          case _ => Process.eval(Task.now(()))
        }
    ).join
  }

  def main(args: Array[String]) {
    (for {
      _ <- run(args).run
    } yield ()).run
  }
}
