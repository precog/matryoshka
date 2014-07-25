package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

// import slamdata.engine.Error

// import com.mongodb.DBObject

import scalaz._
import Scalaz._

// import slamdata.engine.{RenderTree, Terminal, NonTerminal}
// import slamdata.engine.fp._

sealed trait PipelineSchema {
  def accum(op: PipelineOp): PipelineSchema = op match {
    case (p @ PipelineOp.Project(_)) => p.schema
    case (g @ PipelineOp.Group(_, _)) => g.schema
    case _ => this
  }
}
object PipelineSchema {
  import ExprOp.DocVar
  import PipelineOp.{Reshape, Project}

  def apply(ops: List[PipelineOp]): PipelineSchema = ops.foldLeft[PipelineSchema](Init)((s, o) => s.accum(o))

  case object Init extends PipelineSchema
  case class Succ(proj: ListMap[BsonField.Leaf, Unit \/ Succ]) extends PipelineSchema {
    private def toProject0(prefix: DocVar, s: Succ): Project = {
      def rec[A <: BsonField.Leaf](prefix: DocVar, x: A, y: Unit \/ Succ): (A, ExprOp \/ Reshape) = {
        x -> y.fold(_ => -\/ (prefix), s => \/- (s.toProject0(prefix, s).shape))
      }

      val indices = proj.collect {
        case (x : BsonField.Index, y) => 
          rec(prefix \ x, x, y)
      }

      val fields = proj.collect {
        case (x : BsonField.Name, y) =>
          rec(prefix \ x, x, y)
      }

      val arr = Reshape.Arr(indices)
      val doc = Reshape.Doc(fields)

      Project(arr ++ doc)
    }

    def toProject: Project = toProject0(DocVar.ROOT(), this)
  }
}
