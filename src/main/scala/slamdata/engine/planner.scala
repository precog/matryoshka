package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.sql._
import slamdata.engine.fs._

import scalaz.{Node => _, Tree => _, _}
import scalaz.concurrent.{Node => _, _}
import Scalaz._

import scalaz.stream.{Writer => _, _}

trait Planner[PhysicalPlan] {
  def plan(logical: Term[LogicalPlan]): Error \/ PhysicalPlan

  import SQLParser._

  private val sqlParser = new SQLParser()

  private type ProcessTask[A] = Process[Task, A]

  private type WriterResult[A] = Writer[Vector[PhaseResult], A]

  private type EitherWriter[A] = EitherT[WriterResult, Error, A]

  private def withTree[A](name: String)(ea: Error \/ A)(implicit RA: RenderTree[A]): EitherWriter[A] = {
    val result = ea.fold(
      error => PhaseResult.Error(name, error),
      a     => PhaseResult.Tree(name, RA.render(a))
    )

    EitherT[WriterResult, Error, A](WriterT.writer[Vector[PhaseResult], Error \/ A]((Vector.empty :+ result) -> ea))
  }

  private def withString[A](name: String)(a: A)(render: A => Cord): EitherWriter[A] = {
    val result = PhaseResult.Detail(name, render(a).toString)

    EitherT[WriterResult, Error, A](
      WriterT.writer[Vector[PhaseResult], Error \/ A](
        (Vector.empty :+ result) -> \/- (a)))
  }

  def queryPlanner(showNative: PhysicalPlan => Cord)(implicit RA: RenderTree[PhysicalPlan]):
      QueryRequest => (Vector[slamdata.engine.PhaseResult], slamdata.engine.Error \/ PhysicalPlan) = { req =>
    import SemanticAnalysis._

    // TODO: Factor these things out as individual WriterT functions that can be composed.

    (for {
      parsed     <- withTree("SQL AST")(sqlParser.parse(req.query))
      select     <- withTree("SQL AST (paths interpreted)")(interpretPaths(parsed, req.mountPath, req.basePath))
      tree       <- withTree("Annotated Tree")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
      tree       <- withTree("Annotated Tree (variables substituted)")(Variables.substVars[SemanticAnalysis.Annotations](tree, _._2, req.variables))
      logical    <- withTree("Logical Plan")(Compiler.compile(tree))
      simplified <- withTree("Simplified")(\/-(Optimizer.simplify(logical)))
      physical   <- withTree("Physical Plan")(plan(simplified))
      _          <- withString("Mongo")(physical)(showNative)
    } yield physical).run.run
  }
}