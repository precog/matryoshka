package slamdata.engine.physical.mongodb

import slamdata.engine.fp._
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._
import Liskov._

package object optimize {
  object pipeline {
    import ExprOp._
    import PipelineOp._

    def get0(leaves: List[BsonField.Leaf], rs: List[Reshape]): Option[ExprOp \/ Reshape] = {
      (leaves, rs) match {
        case (_, Nil) => Some(-\/ (BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())))

        case (Nil, r :: rs) => inlineProject(r, rs).map(\/- apply)

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/ (d @ DocVar(_, _)) => get0(d.path ++ ls, rs)

          case -\/ (e) => 
            if (ls.isEmpty) fixExpr(rs, e).map(-\/ apply) else None

          case \/- (r) => get0(ls, r :: rs)
        }
      }
    }

    private def fixExpr(rs: List[Reshape], e: ExprOp): Option[ExprOp] = {
      type OptionTramp[X] = OptionT[Free.Trampoline, X]

      def lift[A](o: Option[A]): OptionTramp[A] = OptionT(o.point[Free.Trampoline])

      (e.mapUpM[OptionTramp] {
        case ref @ DocVar(_, _) => 
          lift {
            get0(ref.path, rs).flatMap(_.fold(Some.apply, _ => None))
          }
      }).run.run
    }

    def inlineProject(r: Reshape, rs: List[Reshape]): Option[Reshape] = {
      type MapField[X] = Map[BsonField, X]

      val p = Project(r)

      val map = Traverse[MapField].sequence(p.getAll.toMap.mapValues {
        case d @ DocVar(_, _) => get0(d.path, rs)
        case e => fixExpr(rs, e).map(-\/ apply)
      })

      map.map(vs => p.empty.setAll(vs).shape)
    }

    def inlineGroupProjects(g: WorkflowOp.GroupOp): Option[WorkflowOp.GroupOp] = {
      import ExprOp._

      val (rs, src) = g.src.collectShapes

      type MapField[X] = ListMap[BsonField.Leaf, X]

      val grouped = Traverse[MapField].sequence(ListMap(g.getAll: _*).map { t =>
        val (k, v) = t

        k -> (v match {
          case AddToSet(e)  =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(AddToSet(d))
              case _ => None
            }
          case Push(e)      =>
            fixExpr(rs, e) flatMap {
              case d @ DocVar(_, _) => Some(Push(d))
              case _ => None
            }
          case First(e)     => fixExpr(rs, e).map(First(_))
          case Last(e)      => fixExpr(rs, e).map(Last(_))
          case Max(e)       => fixExpr(rs, e).map(Max(_))
          case Min(e)       => fixExpr(rs, e).map(Min(_))
          case Avg(e)       => fixExpr(rs, e).map(Avg(_))
          case Sum(e)       => fixExpr(rs, e).map(Sum(_))
        })
      })

      val by = g.by.fold(e => fixExpr(rs, e).map(-\/ apply), r => inlineProject(r, rs).map(\/- apply))

      (grouped |@| by)((grouped, by) => WorkflowOp.GroupOp(src, Grouped(grouped), by))
    }
  }
}
