/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slamdata.engine.physical.mongodb

import slamdata.Predef._

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._

import scalaz._
import Scalaz._

package object optimize {
  object pipeline {
    import ExprOp._
    import Workflow._
    import IdHandling._

    private def deleteUnusedFields0(op: Workflow, usedRefs: Option[Set[DocVar]]):
        Workflow = {
      def getRefs[A](op: WorkflowF[Workflow], prev: Option[Set[DocVar]]):
          Option[Set[DocVar]] = op match {
        case $Group(_, _, _)            => Some(refs(op).toSet)
        // FIXME: Since we can’t reliably identify which fields are used by a
        //        JS function, we need to assume they all are, until we hit the
        //        next $Group or $Project.
        case $Map(_, _, _)             => None
        case $SimpleMap(_, _, _)       => None
        case $FlatMap(_, _, _)         => None
        case $Reduce(_, _, _)          => None
        case $Project(_, _, IncludeId) => Some(refs(op).toSet + IdVar)
        case $Project(_, _, _)         => Some(refs(op).toSet)
        case $FoldLeft(_, _)           => prev.map(_ + IdVar)
        case _                         => prev.map(_ ++ refs(op))
      }

      def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] =
        defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))

      def getDefs[A](op: WorkflowF[A]): Set[DocVar] = (op match {
        case p @ $Project(_, _, _)      => p.getAll.map(_._1)
        case g @ $Group(_, _, _)        => g.getAll.map(_._1)
        case s @ $SimpleMap(_, _, _)    => s.getAll.getOrElse(Nil)
        case _                          => Nil
      }).map(DocVar.ROOT(_)).toSet

      val pruned =
        usedRefs.fold(op.unFix) { usedRefs =>
          val unusedRefs =
            unused(getDefs(op.unFix), usedRefs).toList.flatMap(_.deref.toList)
          op.unFix match {
            case p @ $Project(_, _, _)      =>
              val p1 = p.deleteAll(unusedRefs)
              if (p1.shape.value.isEmpty) p1.src.unFix
              else p1
            case g @ $Group(_, _, _)        => g.deleteAll(unusedRefs.map(_.flatten.head))
            case s @ $SimpleMap(_, _, _)    => s.deleteAll(unusedRefs)
            case o                          => o
          }
        }

      Term(pruned.map(deleteUnusedFields0(_, getRefs(pruned, usedRefs))))
    }

    def deleteUnusedFields(op: Workflow) = deleteUnusedFields0(op, None)

    def reorderOps(op: Workflow): Workflow = {
      def go(op: Workflow): Option[Workflow] = op.unFix match {
        case $Skip(Term($Project(src0, shape, id)), count) =>
          Some(chain(src0,
            $skip(count),
            $project(shape, id)))
        case $Skip(Term($SimpleMap(src0, fn @ NonEmptyList(MapExpr(_)), scope)), count) =>
          Some(chain(src0,
            $skip(count),
            $simpleMap(fn, scope)))

        case $Limit(Term($Project(src0, shape, id)), count) =>
          Some(chain(src0,
            $limit(count),
            $project(shape, id)))
        case $Limit(Term($SimpleMap(src0, fn @ NonEmptyList(MapExpr(_)), scope)), count) =>
          Some(chain(src0,
            $limit(count),
            $simpleMap(fn, scope)))

        case m @ $Match(Term(p @ $Project(src0, shape, id)), sel) =>
          val defs = p.getAll.collect { case (n, x @ DocVar(_, _)) =>
            DocField(n) -> x
          }.toMap
          if (refs(m).toSet subsetOf defs.keys.toSet)
            Some(chain(src0,
              $match(sel.mapUpFields { case f => val DocVar(_, Some(d)) = defs(DocField(f)); d }),
              $project(shape, id)))
          else None

        case m @ $Match(Term(p @ $SimpleMap(src0, fn @ NonEmptyList(MapExpr(jsFn)), scope)), sel) => {
          import slamdata.engine.javascript._
          import JsCore._
          def loop(expr: Term[JsCore]): Map[DocVar, DocVar] =
          expr.simplify.unFix match {
            case Obj(values) =>
              values.toList.collect {
                case (n, Term(jsFn.base)) => DocField(BsonField.Name(n)) -> DocVar.ROOT()
                case (n, Term(Access(Term(jsFn.base), Term(Literal(Js.Str(x)))))) => DocField(BsonField.Name(n)) -> DocField(BsonField.Name(x))
              }.toMap
            case SpliceObjects(srcs) => srcs.map(loop).foldLeft(Map[DocVar, DocVar]())(_++_)
            case _ => Map.empty
          }
          val defs = loop(jsFn.expr)
          if (refs(m).toSet subsetOf defs.keys.toSet)
            Some(chain(src0,
              $match(sel.mapUpFields { case f => val DocVar(_, Some(d)) = defs(DocField(f)); d }),
              $simpleMap(fn, scope)))
          else None
        }

        // NB: re-ordering can put ops next to each other that can be coalesced (typically, $projects).
        case _ =>
          val p1 = coalesce(op)
          if (p1 != op) Some(p1) else None
      }

      op.rewrite(go)
    }

    def get0(leaves: List[BsonField.Leaf], rs: List[Reshape]): Option[ExprOp \/ Reshape] = {
      (leaves, rs) match {
        case (_, Nil) => Some(-\/ (BsonField(leaves).map(DocVar.ROOT(_)).getOrElse(DocVar.ROOT())))

        case (Nil, r :: rs) => Some(\/-(inlineProject0(r, rs)))

        case (l :: ls, r :: rs) => r.get(l).flatMap {
          case -\/ (Include)          => get0(leaves, rs)
          case -\/ (d @ DocVar(_, _)) => get0(d.path ++ ls, rs)
          case -\/ (e)                =>
            if (ls.isEmpty) fixExpr(rs, e).map(-\/ apply) else None
          case  \/-(r)                => get0(ls, r :: rs)
        }
      }
    }

    private def fixExpr(rs: List[Reshape], e: ExprOp): Option[ExprOp] = {
      type OptionTramp[X] = OptionT[Free.Trampoline, X]

      def lift[A](o: Option[A]): OptionTramp[A] = OptionT(o.point[Free.Trampoline])

      (e.mapUpM[OptionTramp] {
        case ref @ DocVar(_, _) =>
          lift(get0(ref.path, rs).flatMap(_.fold(Some.apply, κ(None))))
      }).run.run
    }

    private def inlineProject0(r: Reshape, rs: List[Reshape]): Reshape =
      inlineProject($Project((), r, IdHandling.IgnoreId), rs)

    def inlineProject[A](p: $Project[A], rs: List[Reshape]): Reshape = {
      val map = p.getAll.map { case (k, v) =>
        k -> (v match {
          case Include          => get0(k.flatten.toList, rs)
          case d @ DocVar(_, _) => get0(d.path, rs)
          case e                => fixExpr(rs, e).map(-\/ apply)
        })
      }.foldLeft[ListMap[BsonField, ExprOp \/ Reshape]](ListMap()) {
        case (acc, (k, v)) => v match {
          case Some(x) => acc + (k -> x)
          case None    => acc
        }
      }

      p.empty.setAll(map).shape
    }

    /** Map from old grouped names to new names and mapping of expressions. */
    def renameProjectGroup(r: Reshape, g: Grouped): Option[ListMap[BsonField.Name, List[BsonField.Name]]] = {
      val s = r.value.toList.map {
        case (newName, -\/(v @ DocVar(_, _))) =>
          v.path match {
            case List(oldHead @ BsonField.Name(_)) =>
              g.value.get(oldHead).map { κ(oldHead -> newName) }
            case _ => None
          }
        case _ => None
      }

      def multiListMap[A, B](ts: List[(A, B)]): ListMap[A, List[B]] =
        ts.foldLeft(ListMap[A,List[B]]()) { case (map, (a, b)) => map + (a -> (map.get(a).getOrElse(List[B]()) :+ b)) }

      s.sequenceU.map(multiListMap)
    }

    def inlineProjectGroup(r: Reshape, g: Grouped): Option[Grouped] = {
      for {
        names   <- renameProjectGroup(r, g)
        values1 = names.flatMap {
          case (oldName, ts) => ts.map((_: BsonField.Leaf) -> g.value(oldName))
        }
      } yield Grouped(values1)
    }

    def inlineProjectUnwindGroup(r: Reshape, unwound: DocVar, g: Grouped): Option[(DocVar, Grouped)] = {
      for {
        names    <- renameProjectGroup(r, g)
        unwound1 <- unwound.path match {
          case (name @ BsonField.Name(_)) :: Nil => names.get(name) match {
            case Some(n :: Nil) => Some(DocField(n))
            case _ => None
          }
          case _ => None
        }
        values1 = names.flatMap {
          case (oldName, ts) => ts.map((_: BsonField.Leaf) -> g.value(oldName))
        }
      } yield unwound1 -> Grouped(values1)
    }

    def inlineGroupProjects(g: $Group[Workflow]):
        Option[(Workflow, Grouped, ExprOp \/ Reshape)] = {
      import ExprOp._

      val (rs, src) = g.src.para(collectShapes)

      val grouped = ListMap(g.getAll: _*).map { t =>
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
      }.sequence

      val by = g.by.fold(e => fixExpr(rs, e).map(-\/(_)), r => Some(\/-(inlineProject0(r, rs))))

      (grouped |@| by)((grouped, by) => (src, Grouped(grouped), by))
    }
  }
}
