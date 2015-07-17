package slamdata.engine.repl

import java.io.IOException
import org.jboss.aesh.console.Console
import org.jboss.aesh.console.AeshConsoleCallback
import org.jboss.aesh.console.ConsoleOperation
import org.jboss.aesh.console.Prompt
import org.jboss.aesh.console.helper.InterruptHook
import org.jboss.aesh.console.settings.SettingsBuilder
import org.jboss.aesh.edit.actions.Action

import slamdata.engine._; import Backend._; import Errors._
import slamdata.engine.fp._
import slamdata.engine.fs._
import slamdata.engine.sql._

import scalaz.concurrent.{Node => _, _}
import scalaz.{Node => _, _}
import Scalaz._
import scalaz.stream._

import slamdata.engine.physical.mongodb.util
import slamdata.engine.config._

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
    val SummaryCountPattern = "(?i)(?:set +)?summaryCount *= *(\\d+)".r
    val SetVarPattern       = "(?i)(?:set +)?(\\w+) *= *(.*\\S)".r
    val UnsetVarPattern     = "(?i)unset +(\\w+)".r
    val ListVarPattern      = "(?i)env".r

    final case object Exit extends Command
    final case object Unknown extends Command
    final case object Help extends Command
    final case object ListVars extends Command
    final case class Cd(dir: Path) extends Command
    final case class Select(name: Option[String], query: String) extends Command
    final case class Ls(dir: Option[Path]) extends Command
    final case class Save(path: Path, value: String) extends Command
    final case class Append(path: Path, value: String) extends Command
    final case class Delete(path: Path) extends Command
    final case class Debug(level: DebugLevel) extends Command
    final case class SummaryCount(rows: Int) extends Command
    final case class SetVar(name: String, value: String) extends Command
    final case class UnsetVar(name: String) extends Command
  }

  private type Printer = String => Task[Unit]

  sealed trait DebugLevel
  object DebugLevel {
    final case object Silent extends DebugLevel
    final case object Normal extends DebugLevel
    final case object Verbose extends DebugLevel

    def fromInt(code: Int): Option[DebugLevel] = code match {
      case 0 => Some(Silent)
      case 1 => Some(Normal)
      case 2 => Some(Verbose)
      case _ => None
    }
  }

  final case class RunState(
    printer:      Printer,
    backend:      Backend,
    path:         Path,
    unhandled:    Option[Command],
    debugLevel:   DebugLevel,
    summaryCount: Int,
    variables:    Map[String, String])

  val codec = DataCodec.Readable  // TODO: make this settable (see #592)

  def targetPath(s: RunState, path: Option[Path]): Path =
    path.flatMap(_.from(s.path).toOption).getOrElse(s.path)

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.Null"))
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
      case SummaryCountPattern(rows)       => SummaryCount(rows.toInt)
      case HelpPattern()                   => Help
      case SetVarPattern(name, value)      => SetVar(name, value)
      case UnsetVarPattern(name)           => UnsetVar(name)
      case ListVarPattern()                => ListVars
      case _                               => Unknown
    }
  }

  private def commandInput: Task[(Printer, Process[Task, Command])] =
    Task.delay {
      val queue = async.unboundedQueue[Command](Strategy.Sequential)

      val console =
        new Console(new SettingsBuilder()
          .parseOperators(false)
          .enableExport(false)
          .interruptHook(new InterruptHook {
            def handleInterrupt(console: Console, action: Action) = {
              queue.enqueueOne(Command.Exit).run
              console.getShell.out.println("exit")
              console.stop
            }
          })
          .create())
      console.setPrompt(new Prompt("💪 $ "))

      val out = (s: String) => Task.delay { console.getShell.out.println(s) }
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
         |   set summaryCount = [rows]
         |   set [var] = [value]
         |   env""".stripMargin)

  def listVars(state: RunState): Task[Unit] =
    state.printer(state.variables.map(t => t._1 + "=" + t._2).mkString("\n"))

  def showError(state: RunState): Task[Unit] =
    state.printer("""|Unrecognized command!""".stripMargin)

  sealed trait EngineError { def message: String }
  type EngineTask[A] = ETask[EngineError, A]
  type EngineProc[A] = Process[EngineTask, A]
  final case class EEnvironmentError(e: EnvironmentError) extends EngineError {
    def message = e.message
  }
  final case class EParsingError(e: ParsingError) extends EngineError {
    def message = e.message
  }
  final case class EPathError(e: PathError) extends EngineError {
    def message = e.message
  }
  final case class EProcessingError(e: ProcessingError) extends EngineError {
    def message = e.message
  }
  final case class EDataEncodingError(e: DataEncodingError)
      extends EngineError {
    def message = e.message
  }
  final case class EWriteError(e: WriteError)
      extends EngineError {
    def message = e.message
  }

  def select(state: RunState, query: String, name: Option[String]):
      EngineProc[Unit] = {
    def summarize(max: Int)(rows: IndexedSeq[Data]): String =
      if (rows.lengthCompare(0) <= 0) "No results found"
      else
        (Prettify.renderTable(rows.take(max).toList) ++
          (if (rows.lengthCompare(max) > 0) "..." :: Nil else Nil)).mkString("\n")

    def timeIt[A](t: Task[A]): Task[(A, Double)] = Task.delay {
      import org.threeten.bp.{Instant, Duration}
      def secondsAndTenths(dur: Duration) = dur.toMillis/100/10.0
      val startTime = Instant.now
      val a = t.run
      val endTime = Instant.now
      a -> secondsAndTenths(Duration.between(startTime, endTime))
    }

    import state.printer

    (for {
      expr <- SQLParser.parseInContext(Query(query), state.path)
    } yield {
        val (log, resultT) = state.backend.eval(QueryRequest(expr, name.map(Path(_)), Variables.fromMap(state.variables)))
        Process.eval[ETask[ProcessingError, ?], Unit](liftE[ProcessingError](state.debugLevel match {
          case DebugLevel.Silent  => Task.now(())
          case DebugLevel.Normal  => printer(log.takeRight(1).mkString("\n\n") + "\n")
          case DebugLevel.Verbose => printer(log.mkString("\n\n") + "\n")
                                                                                    })) ++
          Process.eval[ETask[ProcessingError, ?], Unit](liftE[ProcessingError](timeIt(resultT.runLog.run)).flatMap { case (results, elapsed) =>
            for {
              _       <- liftE(printer("Query time: " + elapsed + "s"))
              preview <- EitherT(Task.now(results.map(_.take(state.summaryCount + 1))))
              _       <- liftE(printer(summarize(state.summaryCount)(preview)))
            } yield ()
          })
    }).fold(
      e => EitherT.left(Task.now(EParsingError(e))),
      _.leftMap(EProcessingError))
  }

  def ls(state: RunState, path: Option[Path]): PathTask[Unit] =
    state.backend.ls(targetPath(state, path)).flatMap(paths =>
      liftP(state.printer(paths.mkString("\n"))))

  def save(state: RunState, path: Path, value: String): PathTask[Unit] =
    DataCodec.parse(value)(DataCodec.Precise).toOption.map { data =>
      state.backend.save(targetPath(state, Some(path)), Process.emit(data))
    }.getOrElse(EitherT.right(state.printer("parse error")))

  def append(state: RunState, path: Path, value: String): EngineTask[Unit] =
    DataCodec.parse(value)(DataCodec.Precise).fold[EngineTask[Unit]](
      e => EitherT.left(Task.now(EDataEncodingError(e))),
      data => {
        val errors = state.backend.append(targetPath(state, Some(path)), Process.emit(data)).runLog
        errors
          .leftMap[EngineError](EPathError(_))
          .flatMap(_.headOption.fold(
            ().point[EngineTask])(
            e => EitherT.left(Task.now(EWriteError(e)))))
      })

  def delete(state: RunState, path: Path): PathTask[Unit] =
    state.backend.delete(targetPath(state, Some(path)))

  def showDebugLevel(state: RunState, level: DebugLevel): Task[Unit] =
    state.printer(s"""|Set debug level: $level""".stripMargin)

  def showSummaryCount(state: RunState, rows: Int): Task[Unit] =
    state.printer(s"""|Set rows to show in result: $rows""".stripMargin)

  def run(args: Array[String]): Process[EngineTask, Unit] = {
    import Command._

    Process.eval[EngineTask, EngineProc[Unit]](for {
      tuple   <- liftE[EngineError](commandInput)
      (printer, commands) = tuple
      backend <- liftE[EngineError](Config.loadOrEmpty(args.headOption)).flatMap(Mounter.mount(_).leftMap(EEnvironmentError))
    } yield
      commands.translate[ETask[EngineError, ?]](liftE[EngineError]).scan(RunState(printer, backend, Path.Root, None, DebugLevel.Normal, 10, Map()))((state, input) =>
        input match {
          case Cd(path)     =>
            state.copy(
              path      = targetPath(state, Some(path)).asDir,
              unhandled = None)
          case Debug(level) =>
            state.copy(debugLevel = level, unhandled = some(Debug(level)))
          case SummaryCount(rows) =>
            state.copy(summaryCount = rows, unhandled = some(SummaryCount(rows)))
          case SetVar(n, v) =>
            state.copy(variables = state.variables + (n -> v), unhandled = None)
          case UnsetVar(n)  =>
            state.copy(variables = state.variables - n, unhandled = None)
          case _            => state.copy(unhandled = Some(input))
        }).flatMap[EngineTask, Unit] {
        case s @ RunState(_, _, path, Some(command), _, _, _) => command match {
          case Save(path, v)   => Process.eval[EngineTask, Unit](save(s, path, v).leftMap(EPathError))
          case Exit            => Process.Halt(Cause.Kill)
          case Help            => Process.eval[EngineTask, Unit](liftE[EngineError](showHelp(s)))
          case Select(n, q)    => select(s, q, n)
          case Ls(dir)         => Process.eval[EngineTask, Unit](ls(s, dir).leftMap(EPathError))
          case Append(path, v) => Process.eval[EngineTask, Unit](append(s, path, v))
          case Delete(path)    => Process.eval[EngineTask, Unit](delete(s, path).leftMap(EPathError))
          case Debug(level)    => Process.eval[EngineTask, Unit](liftE[EngineError](showDebugLevel(s, level)))
          case SummaryCount(rows) => Process.eval[EngineTask, Unit](liftE[EngineError](showSummaryCount(s, rows)))
          case _               => Process.eval[EngineTask, Unit](liftE[EngineError](showError(s)))
        }
        case _ => Process.eval[EngineTask, Unit](().point[EngineTask])
      }).join
  }

  def main(args: Array[String]): Unit =
    run(args).run.run.handle {
      case e =>
        // An exception was thrown during evaluation; we cannot recover
        // any logging that might have been done, but at least we can
        // capture the stack trace to aid debugging:
        println("A generic error occurred during evaluation of the query")
        println(e.getMessage + "/n" + JavaUtil.abbrev(e.getStackTrace))
        \/-(())
    }.run.fold(
      e => {
        println("A SlamData-specific error occurred during evaluation of the query")
        println(e.message)
      },
      ɩ)
}
