package slamdata.engine.physical.mongodb

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._
import scala.collection.immutable.ListMap

import scalaz._
import Scalaz._
import Liskov._

package object optimize {
  object pipeline {
    import ExprOp._
    import Workflow._
    import IdHandling._

    def deleteUnusedFields(op: Workflow, usedRefs: Option[Set[DocVar]]):
        Workflow = {
      def getRefs[A](op: WorkflowF[Workflow], prev: Option[Set[DocVar]]):
          Option[Set[DocVar]] = op match {
        // Don't count unwinds (if the var isn't referenced elsewhere, it's
        // effectively unused)
        case $Unwind(_, _)             => prev
        case $Group(_, _, _)           => Some(refs(op).toSet)
        // FIXME: Since we can’t reliably identify which fields are used by a
        //        JS function, we need to assume they all are, until we hit the
        //        next $Group or $Project.
        case $Map(_, _)                => None
        case $SimpleMap(_, _)          => None
        case $FlatMap(_, _)            => None
        case $Reduce(_, _)             => None
        case $Project(_, _, IncludeId) => Some(refs(op).toSet + IdVar)
        case $Project(_, _, _)         => Some(refs(op).toSet)
        case $FoldLeft(_, _)           => prev.map(_ + IdVar)
        case $Join(_)                  => prev.map(_ + IdVar)
        case _                         => prev.map(_ ++ refs(op))
      }

      def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] =
        defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))

      def getDefs[A](op: WorkflowF[A]): Set[DocVar] = (op match {
        case p @ $Project(_, _, _) => p.getAll.map(_._1)
        case g @ $Group(_, _, _)   => g.getAll.map(_._1)
        case _                     => Nil
      }).map(DocVar.ROOT(_)).toSet

      val pruned =
        usedRefs.fold(op.unFix) { usedRefs =>
          val unusedRefs =
            unused(getDefs(op.unFix), usedRefs).toList.flatMap(_.deref.toList)
          op.unFix match {
            case p @ $Project(_, _, _) => p.deleteAll(unusedRefs)
            case g @ $Group(_, _, _)   => g.deleteAll(unusedRefs.map(_.flatten.head))
            case o                     => o
          }
        }

      Term(pruned.map(deleteUnusedFields(_, getRefs(pruned, usedRefs))))
    }

    def get0(leaves: List[BsonField.Leaf], rs: List[Reshape]): Option[ExprOp \/ Reshape] = {
      (leaves, rs) match {
        case (_, Nil) => Some(-\/ (BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())))

        case (Nil, r :: rs) => inlineProject(r, rs).map(\/- apply)

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/(d @ DocVar(_, _)) => get0(d.path ++ ls, rs)

          case -\/(e) => 
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
      type MapField[X] = ListMap[BsonField, X]

      val p = $Project((), r, IdHandling.IgnoreId)

      val map = Traverse[MapField].sequence(ListMap(p.getAll: _*).map { case (k, v) =>
        k -> (v match {
          case d @ DocVar(_, _) => get0(d.path, rs)
          case e => fixExpr(rs, e).map(-\/ apply)
        })
      })

      map.map(vs => p.empty.setAll(vs).shape)
    }
    
    /** Map from old grouped names to new names and mapping of expressions. */
    def renameProjectGroup(r: Reshape, g: Grouped): Option[ListMap[BsonField.Name, List[(BsonField.Name, DocVar => DocVar)]]] = {
      val s = r match {
        case Reshape.Doc(value) => 
          value.toList.map {
            case (newName, -\/ (v @ DocVar(_, _))) =>
              v.path match {
                case (oldHead @ BsonField.Name(_)) :: oldTail => 
                  g.value.get(oldHead).map { op =>
                    (oldHead -> (newName, (v: DocVar) => DocVar.ROOT(BsonField(v.path ++ oldTail))))
                  }
              
                case _ => None
              }

            case _ => None
          }
        
        case _ => None :: Nil
      }

      def multiListMap[A, B](ts: List[(A, B)]): ListMap[A, List[B]] =
        ts.foldLeft(ListMap[A,List[B]]()) { case (map, (a, b)) => map + (a -> (map.get(a).getOrElse(List[B]()) :+ b)) }

      s.sequenceU.map(multiListMap)
    }
    
    
    def inlineProjectGroup(r: Reshape, g: Grouped): Option[Grouped] = {
      for {
        names   <- renameProjectGroup(r, g)
        values1 = names.flatMap { case (oldName, ts ) => ts.map { case (newName, f) =>
            (newName: BsonField.Leaf) -> g.value(oldName).mapUp { case v @ DocVar(_,_) => f(v) }.asInstanceOf[GroupOp]
          }}
      } yield Grouped(values1)
    }

    def inlineProjectUnwindGroup(r: Reshape, unwound: DocVar, g: Grouped): Option[(DocVar, Grouped)] = {
      for {
        names    <- renameProjectGroup(r, g)
        unwound1 <- unwound.path match {
          case (name @ BsonField.Name(_)) :: Nil => names.get(name) match {
            case Some((n, _) :: Nil) => Some(DocField(n))
            case _ => None
          }
          case _ => None 
        }
        values1 = names.flatMap { case (oldName, ts ) => ts.map { case (newName, f) =>
            (newName: BsonField.Leaf) -> g.value(oldName).mapUp { case v @ DocVar(_,_) => f(v) }.asInstanceOf[GroupOp]
          }}
      } yield unwound1 -> Grouped(values1)
    }

    def inlineGroupProjects(g: $Group[Workflow]):
        Option[(Workflow, Grouped, ExprOp \/ Reshape)] = {
      import ExprOp._

      val (rs, src) = collectShapes(g.src)

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

      (grouped |@| by)((grouped, by) => (src, Grouped(grouped), by))
    }
  }
}
