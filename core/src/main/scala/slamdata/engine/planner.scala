package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import slamdata.engine.sql._

import scalaz.{Node => _, Tree => _, _}
import Scalaz._

trait Planner[PhysicalPlan] {
  def plan(logical: Term[LogicalPlan]): Error \/ PhysicalPlan

  private val sqlParser = new SQLParser()

  private type WriterResult[A] = Writer[Vector[PhaseResult], A]

  private type EitherWriter[A] = EitherT[WriterResult, Error, A]

  private def withTree[A](name: String)(ea: Error \/ A)(implicit RA: RenderTree[A]): EitherWriter[A] = {
    val result = ea.fold(
      PhaseResult.Error(name, _),
      a => PhaseResult.Tree(name, RA.render(a)))

    EitherT[WriterResult, Error, A](WriterT.writer((Vector.empty[PhaseResult] :+ result) -> ea))
  }

  private def withString[A](a: A)(render: A => (String, Cord)): EitherWriter[A] = {
    val (name, plan) = render(a)
    val result = PhaseResult.Detail(name, plan.toString)

    EitherT[WriterResult, Error, A](
      WriterT.writer[Vector[PhaseResult], Error \/ A](
        (Vector.empty :+ result) -> \/- (a)))
  }

  def queryPlanner(showNative: PhysicalPlan => (String, Cord))(implicit RA: RenderTree[PhysicalPlan]):
      QueryRequest => (Vector[slamdata.engine.PhaseResult], slamdata.engine.Error \/ PhysicalPlan) = { req =>
    import SemanticAnalysis._

    // TODO: Factor these things out as individual WriterT functions that can be composed.

    (for {
      // parsed     <- withTree("SQL AST")(sqlParser.parse(req.query))
      select     <- withTree("SQL AST")(\/-(req.query))
      tree       <- withTree("Annotated Tree")(AllPhases(tree(select)).disjunction.leftMap(ManyErrors.apply))
      tree       <- withTree("Annotated Tree (variables substituted)")(Variables.substVars[SemanticAnalysis.Annotations](tree, _._2, req.variables))
      logical    <- withTree("Logical Plan")(Compiler.compile(tree))
      simplified <- withTree("Simplified")(\/-(logical.cata(Optimizer.simplify)))
      physical   <- withTree("Physical Plan")(plan(simplified))
      _          <- withString(physical)(showNative)
    } yield physical).run.run
  }
}
