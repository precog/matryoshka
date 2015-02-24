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
    val ExitPattern         = "(?i)(?:exit)|(?:quit)".r
    val HelpPattern         = "(?i)(?:help)|(?:commands)|\\?".r
    val CdPattern           = "(?i)cd(?: +(.+))?".r
    val SelectPattern       = "(?i)(select +.+)".r
    val NamedSelectPattern  = "(?i)(\\w+) *:= *(select +.+)".r
    val LsPattern           = "(?i)ls(?: +(.+))?".r
    val SavePattern         = "(?i)save +([\\S]+) (.+)".r
    val AppendPattern       = "(?i)append +([\\S]+) (.+)".r
    val DeletePattern       = "(?i)rm +([\\S]+)".r
    val DebugPattern        = "(?i)(?:set +)?debug *= *(0|1|2)".r
    val SetVarPattern       = "(?i)(?:set +)?(\\w+) *= *(.*\\S)".r
    val UnsetVarPattern     = "(?i)unset +(\\w+)".r
    val ListVarPattern      = "(?i)env".r

    case object Exit extends Command
    case object Unknown extends Command
    case object Help extends Command
    case object ListVars extends Command
    case class Cd(dir: Path) extends Command
    case class Select(name: Option[String], query: String) extends Command
    case class Ls(dir: Option[Path]) extends Command
    case class Save(path: Path, value: String) extends Command
    case class Append(path: Path, value: String) extends Command
    case class Delete(path: Path) extends Command
    case class Debug(level: DebugLevel) extends Command
    case class SetVar(name: String, value: String) extends Command
    case class UnsetVar(name: String) extends Command
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

  case class RunState(
    printer:    Printer,
    mounted:    FSTable[Backend],
    path:       Path = Path.Root,
    unhandled:  Option[Command] = None,
    debugLevel: DebugLevel = DebugLevel.Normal,
    variables:  Map[String, String] = Map())

  val codec = DataCodec.Readable  // TODO: make this settable (see #592)

  def targetPath(s: RunState, path: Option[Path]): Path =
    path.flatMap(_.from(s.path).toOption).getOrElse(s.path)

  private def parseCommand(input: String): Command = {
    import Command._

    input match {
      case ExitPattern()                   => Exit
      case CdPattern(path)                 =>
        Cd(
          if (path == null || path.trim.length == 0) Path.Root
          else Path(path.trim))
      case SelectPattern(query)            => Select(None, query)
      case NamedSelectPattern(name, query) => Select(Some(name), query)
      case LsPattern(path)                 =>
        Ls(
          if (path == null || path.trim.length == 0) None
          else Some(Path(path.trim)))
      case SavePattern(path, value)        => Save(Path(path), value)
      case AppendPattern(path, value)      => Append(Path(path), value)
      case DeletePattern(path)             => Delete(Path(path))
      case DebugPattern(code)              =>
        Debug(DebugLevel.fromInt(code.toInt).getOrElse(DebugLevel.Normal))
      case HelpPattern()                   => Help
      case SetVarPattern(name, value)      => SetVar(name, value)
      case UnsetVarPattern(name)           => UnsetVar(name)
      case ListVarPattern()                => ListVars
      case _                               => Unknown
    }
  }

  private def commandInput: Task[(Printer, Process[Task, Command])] =
    Task.delay {
      val console =
        new Console(new SettingsBuilder().parseOperators(false).enableExport(false).create())
      console.setPrompt(new Prompt("💪 $ "))

      val out = (s: String) => Task.delay { console.getShell.out().println(s) }
      val queue = async.unboundedQueue[Command](Strategy.Sequential)
      console.setConsoleCallback(new AeshConsoleCallback() {
        override def execute(input: ConsoleOperation): Int = {
          val command = parseCommand(input.getBuffer.trim)
          command match {
            case Command.Exit => console.stop()
            case _            => ()
          }
          queue.enqueueOne(command).run
          0
        }
      })

      console.start()

      (out, queue.dequeue)
    }

  def showHelp(state: RunState): Task[Unit] =
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
         |   save [path] [value]
         |   append [path] [value]
         |   rm [path]
         |   set debug = [level]
         |   set [var] = [value]
         |   env""".stripMargin)

  def listVars(state: RunState): Task[Unit] =
    state.printer(state.variables.map(t => t._1 + "=" + t._2).mkString("\n"))

  def showError(state: RunState): Task[Unit] =
    state.printer("""|Unrecognized command!""".stripMargin)

  def select(state: RunState, query: String, name: Option[String]): Process[Task, Unit] =
  {
    def summarize[A](max: Int)(lines: IndexedSeq[A]): String =
      if (lines.lengthCompare(0) <= 0) "No results found"
      else
        (lines.take(max) ++
          (if (lines.lengthCompare(max) > 0) "..." :: Nil else Nil)).mkString("\n")

    def timeIt[A](t: Task[A]): Task[(A, Double)] = Task.delay {
      import org.threeten.bp.{Instant, Duration}
      def secondsAndTenths(dur: Duration) = dur.toMillis/100/10.0
      val startTime = Instant.now
      val a = t.run
      val endTime = Instant.now
      a -> secondsAndTenths(Duration.between(startTime, endTime))
    }

    state.mounted.lookup(state.path).map { case (backend, mountPath, _) =>
      import state.printer

      val (log, resultT) = backend.eval(QueryRequest(Query(query), name.map(Path(_)), mountPath, state.path, Variables.fromMap(state.variables)))
      Process.eval(state.debugLevel match {
        case DebugLevel.Silent  => Task.now(())
        case DebugLevel.Normal  => printer(log.takeRight(1).mkString("\n\n") + "\n")
        case DebugLevel.Verbose => printer(log.mkString("\n\n") + "\n")
      }) ++
      Process.eval(timeIt(resultT) flatMap { case (results, elapsed) =>
        for {
            _ <- printer("Query time: " + elapsed + "s")

            rendered = results.map(data => DataCodec.render(data)(codec).fold(err => err.toString, identity))
            preview <- (rendered |> process1.take(10 + 1)).runLog

            _ <- printer(summarize(10)(preview))
          } yield ()
        }) handle {
          case e : slamdata.engine.Error => Process.eval {
            for {
              _ <- printer("A SlamData-specific error occurred during evaluation of the query")
              _ <- printer(e.fullMessage)
            } yield ()
          }
          case e => Process.eval {
            // An exception was thrown during evaluation; we cannot recover any
            // logging that might have been done, but at least we can capture the
            // stack trace to aid debugging:
            for {
              _ <- printer("A generic error occurred during evaluation of the query")
              _ <- printer(e.getMessage + "/n" + JavaUtil.abbrev(e.getStackTrace))
            } yield ()
          }
        }
    }.getOrElse(Process.eval(state.printer("There is no database mounted to the path " + state.path)))
  }

  def ls(state: RunState, path: Option[Path]): Task[Unit] =
    state.mounted.lookup(targetPath(state, path).asDir).map {
      case (backend, _, relPath) =>
        backend.dataSource.ls(relPath).flatMap { paths =>
          state.printer(paths.mkString("\n"))
        }
    }.getOrElse(state.printer(state.mounted.children(state.path).mkString("\n")))

  def save(state: RunState, path: Path, value: String): Task[Unit] =
    (DataCodec.parse(value)(DataCodec.Precise).toOption |@| state.mounted.lookup(targetPath(state, Some(path)))) {
      case (data, (backend, _, relPath)) =>
        backend.dataSource.save(relPath, Process.emit(data))
    }.getOrElse(state.printer("bad path")) // TODO: report parse error

  def append(state: RunState, path: Path, value: String): Task[Unit] =
    (DataCodec.parse(value)(DataCodec.Precise).toOption |@| state.mounted.lookup(targetPath(state, Some(path)))) {
      case (data, (backend, _, relPath)) =>
        val errors = backend.dataSource.append(relPath, Process.emit(data)).runLog
        errors.run.headOption.map(Task.fail(_)).getOrElse(Task.now(()))
    }.getOrElse(state.printer("bad path")) // TODO: report parse error

  def delete(state: RunState, path: Path): Task[Unit] =
    state.mounted.lookup(targetPath(state, Some(path))).map {
      case (backend, _, relPath) => backend.dataSource.delete(relPath)
    }.getOrElse(state.printer("bad path"))

  def showDebugLevel(state: RunState, level: DebugLevel): Task[Unit] =
    state.printer(s"""|Set debug level: $level""".stripMargin)

  def run(args: Array[String]): Process[Task, Unit] = {
    import Command._

    Process.eval(for {
      tuple   <- commandInput
      (printer, commands) = tuple
      mounted <- args.headOption.map(Config.fromFile _)
                     .getOrElse(Task.now(Config.DefaultConfig))
                     .flatMap(Mounter.mount(_))
    } yield
      commands.scan(RunState(printer, mounted)) { (state, input) =>
        input match {
          case Cd(path)     =>
            state.copy(
              path      = targetPath(state, Some(path)).asDir,
              unhandled = None)
          case Debug(level) =>
            state.copy(debugLevel = level, unhandled = some(Debug(level)))
          case SetVar(n, v) =>
            state.copy(variables = state.variables + (n -> v), unhandled = None)
          case UnsetVar(n)  =>
            state.copy(variables = state.variables - n, unhandled = None)
          case _            => state.copy(unhandled = Some(input))
        }}.flatMap {
        case s @ RunState(_, _, path, Some(command), _, _) => command match {
          case Exit            => Process.Halt(Cause.Kill)
          case Help            => Process.eval(showHelp(s))
          case Select(n, q)    => select(s, q, n)
          case Ls(dir)         => Process.eval(ls(s, dir))
          case Save(path, v)   => Process.eval(save(s, path, v))
          case Append(path, v) => Process.eval(append(s, path, v))
          case Delete(path)    => Process.eval(delete(s, path))
          case Debug(level)    => Process.eval(showDebugLevel(s, level))
          case _               => Process.eval(showError(s))
        }
        case _ => Process.eval(Task.now(()))
      }).join
  }

  def main(args: Array[String]) = run(args).run.run
}
