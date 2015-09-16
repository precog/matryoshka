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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{RenderTree, RenderedTree, Terminal, NonTerminal}, RenderTree.ops._
import quasar.recursionschemes._, Recursive.ops._
import quasar.fp._
import quasar.fs.Path
import optimize.pipeline._
import quasar.javascript._, Js._
import quasar.jscore, jscore.{JsCore, JsFn}
import WorkflowTask._

import monocle.syntax._
import scalaz._, Scalaz._
import shapeless.contrib.scalaz.instances._

sealed trait IdHandling
object IdHandling {
  final case object ExcludeId extends IdHandling
  final case object IncludeId extends IdHandling
  final case object IgnoreId extends IdHandling

  implicit val IdHandlingMonoid = new Monoid[IdHandling] {
    // this is the `coalesce` function
    def append(f1: IdHandling, f2: => IdHandling) = (f1, f2) match {
      case (_, IgnoreId) => f1
      case (_, _)        => f2
    }

    def zero = IgnoreId
  }
}

/**
  A Workflow is a graph of atomic operations, with WorkflowOps for the vertices.
  We crush them down into a WorkflowTask. This `crush` gives us a location to
  optimize our workflow decisions. EG, A sequence of simple ops may be combined
  into a single pipeline request, but if one of those operations contains JS, we
  have to execute that outside of a pipeline, possibly reordering the other
  operations to avoid having two pipelines with a JS operation in the middle.

  We also implement the optimizations at
  http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so that
  we can build others potentially on top of them (including reordering
  non-pipelines around pipelines, etc.).
  */
sealed trait WorkflowF[+A]
object Workflow {
  import quasar.physical.mongodb.accumulator._
  import quasar.physical.mongodb.expression._
  import IdHandling._
  import MapReduce._

  type Workflow = Fix[WorkflowF]
  type WorkflowOp = Workflow => Workflow
  type PipelineOp = PipelineF[Unit]

  val ExprLabel  = "value"
  val ExprName   = BsonField.Name(ExprLabel)
  val ExprVar    = DocVar.ROOT(ExprName)

  val IdLabel  = "_id"
  val IdName   = BsonField.Name(IdLabel)
  val IdVar    = DocVar.ROOT(IdName)

  sealed trait CardinalExpr[A]
  final case class MapExpr[A](fn: A) extends CardinalExpr[A]
  final case class FlatExpr[A](fn: A) extends CardinalExpr[A]

  implicit val TraverseCardinalExpr = new Traverse[CardinalExpr] {
    def traverseImpl[G[_]: Applicative, A, B](
      fa: CardinalExpr[A])(f: A => G[B]):
        G[CardinalExpr[B]] =
      fa match {
        case MapExpr(e)  => f(e).map(MapExpr(_))
        case FlatExpr(e) => f(e).map(FlatExpr(_))
      }
  }

  implicit val CardinalExprComonad = new Comonad[CardinalExpr] {
    def map[A, B](fa: CardinalExpr[A])(f: A => B): CardinalExpr[B] = fa match {
      case MapExpr(e)  => MapExpr(f(e))
      case FlatExpr(e) => FlatExpr(f(e))
    }

    def cobind[A, B](fa: CardinalExpr[A])(f: CardinalExpr[A] => B):
        CardinalExpr[B] = fa match {
      case MapExpr(_)  => MapExpr(f(fa))
      case FlatExpr(_) => FlatExpr(f(fa))
    }

    def copoint[A](p: CardinalExpr[A]) = p match {
      case MapExpr(e)  => e
      case FlatExpr(e) => e
    }
  }

  implicit val PipelineFTraverse = new Traverse[PipelineF] {
    def traverseImpl[G[_], A, B](fa: PipelineF[A])(f: A => G[B])
      (implicit G: Applicative[G]):
        G[PipelineF[B]] = fa match {
      case $Match(src, sel)         => G.apply(f(src))($Match(_, sel))
      case $Project(src, shape, id) => G.apply(f(src))($Project(_, shape, id))
      case $Redact(src, value)      => G.apply(f(src))($Redact(_, value))
      case $Limit(src, count)       => G.apply(f(src))($Limit(_, count))
      case $Skip(src, count)        => G.apply(f(src))($Skip(_, count))
      case $Unwind(src, field)      => G.apply(f(src))($Unwind(_, field))
      case $Group(src, grouped, by) => G.apply(f(src))($Group(_, grouped, by))
      case $Sort(src, value)        => G.apply(f(src))($Sort(_, value))
      case $GeoNear(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
        G.apply(f(src))($GeoNear(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
      case $Out(src, col)           => G.apply(f(src))($Out(_, col))
    }
  }

  implicit val WorkflowFTraverse = new Traverse[WorkflowF] {
    def traverseImpl[G[_], A, B](fa: WorkflowF[A])(f: A => G[B])
      (implicit G: Applicative[G]):
        G[WorkflowF[B]] = fa match {
      case x @ $Pure(_)             => G.point(x)
      case x @ $Read(_)             => G.point(x)
      case $Map(src, fn, scope)     => G.apply(f(src))($Map(_, fn, scope))
      case $FlatMap(src, fn, scope) => G.apply(f(src))($FlatMap(_, fn, scope))
      case $SimpleMap(src, exprs, scope) =>
        G.apply(f(src))($SimpleMap(_, exprs, scope))
      case $Reduce(src, fn, scope)  => G.apply(f(src))($Reduce(_, fn, scope))
      case $FoldLeft(head, tail)    =>
        G.apply2(
          f(head), Traverse[NonEmptyList].sequence(tail.map(f)))(
          $FoldLeft(_, _))
      // NB: Would be nice to replace the rest of this impl with the following
      //     line, but the invariant definition of Traverse doesn’t allow it.
      // case p: PipelineF[_]           => PipelineFTraverse.traverseImpl(p)(f)
      case $Match(src, sel)         => G.apply(f(src))($Match(_, sel))
      case $Project(src, shape, id) => G.apply(f(src))($Project(_, shape, id))
      case $Redact(src, value)      => G.apply(f(src))($Redact(_, value))
      case $Limit(src, count)       => G.apply(f(src))($Limit(_, count))
      case $Skip(src, count)        => G.apply(f(src))($Skip(_, count))
      case $Unwind(src, field)      => G.apply(f(src))($Unwind(_, field))
      case $Group(src, grouped, by) => G.apply(f(src))($Group(_, grouped, by))
      case $Sort(src, value)        => G.apply(f(src))($Sort(_, value))
      case $GeoNear(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
        G.apply(f(src))($GeoNear(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
      case $Out(src, col)           => G.apply(f(src))($Out(_, col))
    }
  }

  def task(fop: Crystallized): WorkflowTask =
    (WorkflowTask.finish _).tupled(fop.op.para(crush))._2

  val finish: Workflow => Workflow = reorderOps _  >>> deleteUnusedFields _

  def coalesce(op: Workflow): Workflow =
    op.unFix match {
      case $Match(src, selector) => src.unFix match {
        case $Sort(src0, value) =>
          chain(src0, $match(selector), $sort(value))
        case $Match(src0, sel0) =>
          chain(src0, $match(Semigroup[Selector].append(sel0, selector)))
        case _ => op
      }
      case p @ $Project(src, shape, id) => src.unFix match {
        case $Project(src0, shape0, id0) =>
          $project(inlineProject(p, List(shape0)), id0 |+| id)(src0)
        // Would like to inline a $project into a preceding $simpleMap, but
        // This is not safe, because sometimes a $project is inserted after
        // $simpleMap specifically to pull fields out of `value`, and those
        // $project ops need to be preserved.
        // case $SimpleMap(src0, js, flatten, scope) =>
        //   shape.toJs.fold(
        //     κ(op),
        //     jsShape => chain(src0,
        //       $simpleMap(
        //         JsMacro(base =>
        //           jscore.Let(
        //             ListMap("__tmp" -> js(base)),
        //             jsShape(jscore.Ident("__tmp")))),
        //         flatten, scope)))
        case $Group(src, grouped, by) if id != ExcludeId =>
          inlineProjectGroup(shape, grouped).map($group(_, by)(src)).getOrElse(op)
        case $Unwind(Fix($Group(src, grouped, by)), unwound)
            if id != ExcludeId =>
          inlineProjectUnwindGroup(shape, unwound, grouped).map { case (unwound, grouped) =>
            chain(src,
              $group(grouped, by),
              $unwind(unwound))
          }.getOrElse(op)
        case _ => op
      }
      case $Limit(src, count) => src.unFix match {
        case $Limit(src0, count0) =>
          chain(src0, $limit(count0 min count))
        case $Skip(src0, count0) =>
          chain(src0, $limit(count0 + count), $skip(count0))
        case _ => op
      }
      case $Skip(src, count) => src.unFix match {
        case $Skip(src0, count0) => $skip(count0 + count)(src0)
        case _                   => op
      }
      case $Group(src, grouped, \/-($literal(bson))) if bson != Bson.Null =>
        coalesce($group(grouped, \/-($literal(Bson.Null)))(src))
      case op0 @ $Group(_, _, _) =>
        inlineGroupProjects(op0).map(tup => Fix(($Group[Workflow](_, _, _)).tupled(tup))).getOrElse(op)
      case $GeoNear(src, _, _, _, _, _, _, _, _, _) => src.unFix match {
        // FIXME: merge the params
        case $GeoNear(_, _, _, _, _, _, _, _, _, _) => op
        case _                                      => op
      }
      case $Map(src, fn, scope) => src.unFix match {
        case $Map(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).fold(
            op)(
            s => chain(src0, $map($Map.compose(fn, fn0), s)))
        case $FlatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).fold(
            op)(
            s => chain(src0, $flatMap($FlatMap.mapCompose(fn, fn0), s)))
        case _                   => op
      }
      case $FlatMap(src, fn, scope) => src.unFix match {
        case $Map(src0, fn0, scope0)     =>
          Reshape.mergeMaps(scope0, scope).fold(
            op)(
            $flatMap($Map.compose(fn, fn0), _)(src0))
        case $FlatMap(src0, fn0, scope0) =>
          Reshape.mergeMaps(scope0, scope).fold(
            op)(
            $flatMap($FlatMap.kleisliCompose(fn, fn0), _)(src0))
        case _                   => op
      }
      case sm @ $SimpleMap(src, _, _) => src.unFix match {
        case sm0 @ $SimpleMap(_, _, _) => Fix(sm0 >>> sm)
        case _                         => op
      }
      case $FoldLeft(head, tail) => head.unFix match {
        case $FoldLeft(head0, tail0) =>
          $FoldLeft.make(head0, tail0 append tail)
        case _                       => op
      }
      case $Out(src, _) => src.unFix match {
        case $Read(_) => src
        case _        => op
      }
      case _ => op
    }

  def pipeline[A <: PipelineF[Workflow]](op: A):
      Option[(DocVar, WorkflowTask, List[PipelineOp])] =
    op match {
      case $Match(src, selector) =>
        def pipelinable(sel: Selector): Boolean = sel match {
          case Selector.Where(_) => false
          case comp: Selector.CompoundSelector =>
            pipelinable(comp.left) && pipelinable(comp.right)
          case _ => true
        }
        if (pipelinable(selector)) {
          lazy val (base, crushed) = src.para(crush)
          src.unFix match {
            case p: PipelineF[Workflow] => pipeline(p).cata(
              { case (base, up, prev) => Some((base, up, prev :+ rewriteRefs(PipelineFTraverse.void(op), prefixBase(base)))) },

              Some((base, crushed, List(rewriteRefs(PipelineFTraverse.void(op), prefixBase(base))))))
            case _ => Some((base, crushed, List(rewriteRefs(PipelineFTraverse.void(op), prefixBase(base)))))
          }
        }
        else None
      // TODO: Not all $Groups can be pipelined. Need to determine when we may
      //       need the group command or a map/reduce.
      case _ => Some(alwaysPipePipe(op))
    }

  /**
    Returns both the final WorkflowTask as well as a DocVar indicating the base
    of the collection.
    */
  private val crush: WorkflowF[(Fix[WorkflowF], (DocVar, WorkflowTask))] => (DocVar, WorkflowTask) = {
      case $Pure(value) => (DocVar.ROOT(), PureTask(value))
      case $Read(coll)  => (DocVar.ROOT(), ReadTask(coll))
      case op @ $Match((src, rez), selector) =>
        // TODO: If we ever allow explicit request of cursors (instead of
        //       collections), we could generate a FindQuery here.

        lazy val nonPipeline = {
          val (base, crushed) = (WorkflowTask.finish _).tupled(rez)
          (ExprVar,
            MapReduceTask(
              crushed,
              MapReduce(
                $Map.mapFn(base match {
                  case DocVar(DocVar.ROOT, None) => $Map.mapNOP
                  case _                         => $Map.mapProject(base)
                }),
                $Reduce.reduceNOP,
                // TODO: Get rid of this asInstanceOf!
                selection = Some(rewriteRefs(PipelineFTraverse.void(op).asInstanceOf[$Match[Workflow]], prefixBase(base)).selector))))
        }
        pipeline($Match(src, selector)) match {
          case Some((base, up, mine)) => (base, PipelineTask(up, mine))
          case None                   => nonPipeline
        }
      case p: PipelineF[(Fix[WorkflowF], (DocVar, WorkflowTask))] =>
        alwaysPipePipe(p.reparent(p.src._1)) match {
          case (base, up, pipe) => (base, PipelineTask(up, pipe))
        }
      case op @ $Map((_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(_, _, _, _, _, _, None, scope0, _, _)))), fn, scope) =>
        Reshape.mergeMaps(scope0, scope).fold(
          op.newMR(base, src1, None, None, None))(
          s => base -> MapReduceTask(src0,
            mr applyLens MapReduce._finalizer set Some($Map.finalizerFn(fn))
              applyLens MapReduce._scope set s))

      case op @ $Reduce((_, (base, src1 @ MapReduceTask(src0, mr @ MapReduce(_, reduceNOP, _, _, _, _, None, scope0, _, _)))), fn, scope) =>
        Reshape.mergeMaps(scope0, scope).fold(
          op.newMR(base, src1, None, None, None))(
          s => base -> MapReduceTask(src0,
            mr applyLens MapReduce._reduce set fn
              applyLens MapReduce._scope set s))

      case op: MapReduceF[_] =>
        op.src match {
          case (_, (base, PipelineTask(src0, List($Match(_, sel))))) =>
            op.newMR(base, src0, Some(sel), None, None)
          case (_, (base, PipelineTask(src0, List($Sort(_, sort))))) =>
            op.newMR(base, src0, None, Some(sort), None)
          case (_, (base, PipelineTask(src0, List($Limit(_, count))))) =>
            op.newMR(base, src0, None, None, Some(count))
          case (_, (base, PipelineTask(src0, List($Match(_, sel), $Sort(_, sort))))) =>
            op.newMR(base, src0, Some(sel), Some(sort), None)
          case (_, (base, PipelineTask(src0, List($Match(_, sel), $Limit(_, count))))) =>
            op.newMR(base, src0, Some(sel), None, Some(count))
          case (_, (base, PipelineTask(src0, List($Sort(_, sort), $Limit(_, count))))) =>
            op.newMR(base, src0, None, Some(sort), Some(count))
          case (_, (base, PipelineTask(src0, List($Match(_, sel), $Sort(_, sort), $Limit(_, count))))) =>
            op.newMR(base, src0, Some(sel), Some(sort), Some(count))
          case (_, (base, srcTask)) =>
            val (nb, task) = WorkflowTask.finish(base, srcTask)
            op.newMR(nb, task, None, None, None)
        }
      case $FoldLeft(head, tail) =>
        (ExprVar,
          FoldLeftTask(
            (WorkflowTask.finish _).tupled(head._2)._2,
            tail.map(_._2._2 match {
              case MapReduceTask(src, mr) =>
                // FIXME: $FoldLeft currently always reduces, but in future we’ll
                //        want to have more control.
                MapReduceTask(src,
                  mr applyLens MapReduce._out set
                    Some(MapReduce.WithAction(
                      MapReduce.Action.Reduce,
                      db = None,
                      sharded = None,
                      nonAtomic = Some(true))))
              // NB: `finalize` should ensure that the final op is always a
              //     $Reduce.
              case src => scala.sys.error("not a mapReduce: " + src)
            })))
    }

  val collectShapes: WorkflowF[(Workflow, (List[Reshape], Workflow))] => (List[Reshape], Workflow) = {
    case $Project(src, shape, _) =>
      ((x: List[Reshape]) => shape :: x).first(src._2)
    case x                       => (Nil, Fix(x.map(_._1)))
  }

  // helper for rewriteRefs
  def prefixBase(base: DocVar): PartialFunction[DocVar, DocVar] =
    PartialFunction(base \\ _)

  // TODO: Make this a trait, and implement it for actual types, rather than all
  //       in here (already done for ExprOp and Reshape). (#438)
  def rewriteRefs[A <: WorkflowF[_]](
    op: A, applyVar0: PartialFunction[DocVar, DocVar]):
      A = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyMap[A](m: ListMap[BsonField, A]): ListMap[BsonField, A] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyNel[A](m: NonEmptyList[(BsonField, A)]): NonEmptyList[(BsonField, A)] = m.map(t => applyFieldName(t._1) -> t._2)

    def applyFindQuery(q: FindQuery): FindQuery = {
      q.copy(
        query   = applySelector(q.query),
        max     = q.max.map(applyMap _),
        min     = q.min.map(applyMap _),
        orderby = q.orderby.map(applyNel _)
      )
    }

    (op match {
      case $Project(src, shape, xId) =>
        $Project(src, shape.rewriteRefs(applyVar0), xId)
      case $Group(src, grouped, by)  =>
        $Group(src,
          grouped.rewriteRefs(applyVar0),
          by.bimap(_.rewriteRefs(applyVar0), rewriteExprRefs(_)(applyVar0)))
      case $Match(src, s)            => $Match(src, applySelector(s))
      case $Redact(src, e)           => $Redact(src, rewriteExprRefs(e)(applyVar0))
      case $Unwind(src, f)           => $Unwind(src, applyVar(f))
      case $Sort(src, l)             => $Sort(src, applyNel(l))
      case g: $GeoNear[_]            =>
        g.copy(
          distanceField = applyFieldName(g.distanceField),
          query = g.query.map(applyFindQuery _))
      case _                          => op
    }).asInstanceOf[A]
  }

  final def refs[A <: WorkflowF[_]](op: A): List[DocVar] = {
    // FIXME: Sorry world
    val vf = new scala.collection.mutable.ListBuffer[DocVar]
    ignore(rewriteRefs(op, { case v => ignore(vf += v); v }))
    vf.toList
  }

  def rewrite[A <: WorkflowF[_]](op: A, base: DocVar): (A, DocVar) =
    (rewriteRefs(op, prefixBase(base)) -> (op match {
      case $Group(_, _, _)   => DocVar.ROOT()
      case $Project(_, _, _) => DocVar.ROOT()
      case _                 => base
    }))

  def simpleShape(op: Workflow): Option[List[BsonField.Leaf]] = op.unFix match {
    case $Pure(Bson.Doc(value))             => Some(value.keys.toList.map(BsonField.Name))
    case $Project(_, Reshape(value), _)     => Some(value.keys.toList)
    case sm @ $SimpleMap(_, _, _) =>
      def loop(expr: JsCore): Option[List[jscore.Name]] =
        expr.simplify match {
          case jscore.Obj(value)      => Some(value.keys.toList)
          case jscore.Let(_, _, body) => loop(body)
          case _ => None
        }
      loop(sm.simpleExpr.expr).map(_.map(n => BsonField.Name(n.value)))
    case $Group(_, Grouped(value), _)       => Some(value.keys.toList)
    case $Unwind(src, _)                    => simpleShape(src)
    case sp: ShapePreservingF[_]            => simpleShape(sp.src)
    case _ => None
  }

  /** Operations without an input. */
  sealed trait SourceOp extends WorkflowF[Nothing]

  /** Operations with a single source op. */
  sealed trait SingleSourceF[A] extends WorkflowF[A] {
    def src: A
    def reparent[B](newSrc: B): SingleSourceF[B]
    /**
      Reparenting that handles coalescing (but is more restrictive as a result).
      */
    def reparentW[B](newSrc: Workflow): Workflow =
      coalesce(Fix(reparent(newSrc)))
  }

  /**
   * This should be renamed once the other PipelineOp goes away, but it is the
   * subset of operations that can ever be pipelined.
   */
  abstract sealed class PipelineF[A](op: String) extends SingleSourceF[A] {
    override def reparent[B](newSrc: B): PipelineF[B]
    def rhs: Bson
    def bson: Bson.Doc = Bson.Doc(ListMap(op -> rhs))
  }
  abstract sealed class ShapePreservingF[A](op: String) extends PipelineF[A](op)

  /**
   * Flattens the sequence of operations like so:
   *
   *   chain(
   *     $read(Path.fileAbs("foo")),
   *     $match(Selector.Where(Js.Bool(true))),
   *     $limit(7))
   * ==
   *   val read = $read(Path.fileAbs("foo"))
   *   val match = $match(Selector.Where(Js.Bool(true))(read)
   *   $limit(7)(match)
   */
  def chain(src: Workflow, op1: WorkflowOp, ops: (WorkflowOp)*): Workflow =
    ops.foldLeft(op1(src))((s, o) => o(s))

  /** A type for a `Workflow` which has had `crystallize` applied to it. */
  final case class Crystallized(op: Workflow)

  /**
    Performs some irreversible conversions, meant to be used once, after the
    entire workflow has been generated.
    */
  // probable conversions
  // to $Map:          $Project
  // to $FlatMap:      $Match, $Limit (using scope), $Skip (using scope), $Unwind, $GeoNear
  // to $Map/$Reduce:  $Group
  // ???:              $Redact
  // none:             $Sort
  // NB: We don’t convert a $Project after a map/reduce op because it could
  //     affect the final shape unnecessarily.
  def crystallize(op: Workflow): Crystallized = {
    def unwindSrc(uw: $Unwind[Fix[WorkflowF]]): WorkflowF[Fix[WorkflowF]] = uw.src.unFix match {
      case uw1 @ $Unwind(_, _) => unwindSrc(uw1)
      case src => src
    }

    def crystallize0(op: Workflow): Workflow = op.unFix match {
      case mr: MapReduceF[Workflow] => mr.src.unFix match {
        case $Project(src, shape, _)  =>
          shape.toJs.fold(
            κ(op.descend(crystallize0(_))),
            x => {
              val base = jscore.Name("__rez")
              crystallize0(mr.reparentW($simpleMap(NonEmptyList(MapExpr(JsFn(base, x(jscore.Ident(base))))), ListMap())(src)))
            })
        case uw @ $Unwind(_, _) if !unwindSrc(uw).isInstanceOf[PipelineF[_]]
                                      => crystallize0(mr.reparentW(Fix(uw.flatmapop)))
        case sm @ $SimpleMap(_, _, _) => crystallize0(mr.reparentW(Fix(sm.raw)))
        case _                        => op.descend(crystallize0(_))
      }
      case op @ $FoldLeft(head, tail) =>
        $foldLeft(
          crystallize0(chain(
            head,
            $project(Reshape(ListMap(
              ExprName -> \/-($$ROOT))),
              IncludeId))),
          crystallize0(tail.head.unFix match {
            case $Reduce(_, _, _) => tail.head
            case _ => chain(tail.head, $reduce($Reduce.reduceFoldLeft, ListMap()))
          }),
          tail.tail.map(x => crystallize0(x.unFix match {
            case $Reduce(_, _, _) => x
            case _ => chain(x, $reduce($Reduce.reduceFoldLeft, ListMap()))
          })):_*)

      case _ => op.descend(crystallize0)
    }

    val crystallized = crystallize0(finish(op))

    def fixShape(wf: Workflow) =
      Workflow.simpleShape(wf).fold(
        crystallized)(
        n => $project(Reshape(n.map(_.toName -> \/-($include())).toListMap), IgnoreId)(crystallized))

    def promoteKnownShape(wf: Workflow): Workflow = wf.unFix match {
      case $SimpleMap(_, _, _)  => fixShape(wf)
      case sp: ShapePreservingF[_] => promoteKnownShape(sp.src)
      case _                       => crystallized
    }

    Crystallized(promoteKnownShape(crystallized))
  }

  final case class $Pure(value: Bson) extends SourceOp
  def $pure(value: Bson) = coalesce(Fix[WorkflowF]($Pure(value)))

  final case class $Read(coll: Collection) extends SourceOp
  def $read(coll: Collection) = coalesce(Fix[WorkflowF]($Read(coll)))

  final case class $Match[A](src: A, selector: Selector)
      extends ShapePreservingF[A]("$match") {
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = selector.bson
  }
  object $Match {
    def make(selector: Selector)(src: Workflow): Workflow =
      coalesce(Fix($Match(src, selector)))
  }
  val $match = $Match.make _

  private def alwaysPipePipe(op: PipelineF[Workflow]):
      (DocVar, WorkflowTask, Pipeline) = {
    lazy val (base, crushed) = (WorkflowTask.finish _).tupled(op.src.para(crush))
    // TODO: this is duplicated in `WorkflowBuilder.rewrite`
    def repairBase(base: DocVar) = op match {
      case $Group(_, _, _)   => DocVar.ROOT()
      case $Project(_, _, _) => DocVar.ROOT()
      case _                  => base
    }
    op.src.unFix match {
      case p: PipelineF[Workflow] => pipeline(p).cata(
        {
          case (base, up, prev) =>
            val (nb, task) = WorkflowTask.finish(base, up)
            (repairBase(nb),
              task,
              prev :+ rewriteRefs(PipelineFTraverse.void(op), prefixBase(nb)))
        },
        (repairBase(base),
          crushed,
          List(rewriteRefs(PipelineFTraverse.void(op), prefixBase(base)))))
      case _ =>
        (repairBase(base),
          crushed,
          List(rewriteRefs(PipelineFTraverse.void(op), prefixBase(base))))
    }
  }

  final case class $Project[A](src: A, shape: Reshape, idExclusion: IdHandling)
      extends PipelineF[A]("$project") {
    def reparent[B](newSrc: B): $Project[B] = copy(src = newSrc)
    def rhs = idExclusion match {
      case IdHandling.ExcludeId =>
        Bson.Doc(shape.bson.value + (Workflow.IdLabel -> Bson.Bool(false)))
      case _         => shape.bson
    }
    def empty: $Project[A] = $Project.EmptyDoc(src)

    def set(field: BsonField, value: Reshape.Shape): $Project[A] =
      $Project(src,
        shape.set(field, value),
        if (field == IdName) IncludeId else idExclusion)

    def get(ref: DocVar): Option[Reshape.Shape] = ref match {
      case DocVar(_, Some(field)) => shape.get(field)
      case _                      => Some(-\/(shape))
    }

    def getAll: List[(BsonField, Expression)] = {
      val all = Reshape.getAll(shape)
      idExclusion match {
        case IncludeId => all.collectFirst {
          case (IdName, _) => all
        }.getOrElse((IdName, $include()) :: all)
        case _         => all
      }
    }

    def setAll(fvs: Iterable[(BsonField, Reshape.Shape)]): $Project[A] =
      $Project(
        src,
        Reshape.setAll(shape, fvs),
        if (fvs.exists(_._1 == IdName)) IncludeId else idExclusion)

    def deleteAll(fields: List[BsonField]): $Project[A] =
      $Project(src,
        Reshape.setAll(Reshape.EmptyDoc,
          Reshape.getAll(this.shape)
            .filterNot(t => fields.exists(t._1.startsWith(_)))
            .map(t => t._1 -> \/-(t._2))),
        if (fields.contains(IdName)) ExcludeId else idExclusion)

    def id: $Project[A] = {
      def loop(prefix: Option[BsonField], p: $Project[A]): $Project[A] = {
        def nest(child: BsonField): BsonField =
          prefix.map(_ \ child).getOrElse(child)

        $Project(
          p.src,
          Reshape(
            p.shape.value.transform {
              case (k, v) =>
                v.fold(
                  r => -\/(loop(Some(nest(k)), $Project(p.src, r, p.idExclusion)).shape),
                  κ(\/-($var(DocVar.ROOT(nest(k))))))
            }),
          p.idExclusion)
      }

      loop(None, this)
    }
  }
  object $Project {
    def make(shape: Reshape, id: IdHandling)(src: Workflow): Workflow =
      coalesce(Fix($Project(src, shape, id)))

    def EmptyDoc[A](src: A) = $Project(src, Reshape.EmptyDoc, ExcludeId)
  }
  val $project = $Project.make _

  final case class $Redact[A](src: A, value: Expression)
      extends PipelineF[A]("$redact") {
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = value.cata(bsonƒ)
  }
  object $Redact {
    def make(value: Expression)(src: Workflow): Workflow =
      coalesce(Fix($Redact(src, value)))

    val DESCEND = DocVar(DocVar.Name("DESCEND"),  None)
    val PRUNE   = DocVar(DocVar.Name("PRUNE"),    None)
    val KEEP    = DocVar(DocVar.Name("KEEP"),     None)
  }
  val $redact = $Redact.make _

  final case class $Limit[A](src: A, count: Long)
      extends ShapePreservingF[A]("$limit") {
    // TODO: If the preceding is a $Match, and it or its source isn’t
    //       pipelineable, then return a FindQuery combining the match and this
    //       limit
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = Bson.Int64(count)
  }
  object $Limit {
    def make(count: Long)(src: Workflow): Workflow =
      coalesce(Fix($Limit(src, count)))
  }
  val $limit = $Limit.make _

  final case class $Skip[A](src: A, count: Long)
      extends ShapePreservingF[A]("$skip") {
    // TODO: If the preceding is a $Match (or a limit preceded by a $Match),
    //       and it or its source isn’t pipelineable, then return a FindQuery
    //       combining the match and this skip
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = Bson.Int64(count)
  }
  object $Skip {
    def make(count: Long)(src: Workflow): Workflow =
      coalesce(Fix($Skip(src, count)))
  }
  val $skip = $Skip.make _

  final case class $Unwind[A](src: A, field: DocVar)
      extends PipelineF[A]("$unwind") {
    lazy val flatmapop = $SimpleMap(src, NonEmptyList(FlatExpr(field.toJs)), ListMap())
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = field.bson
  }
  object $Unwind {
    def make(field: DocVar)(src: Workflow): Workflow =
      coalesce(Fix($Unwind(src, field)))
  }
  val $unwind = $Unwind.make _

  final case class $Group[A](src: A, grouped: Grouped, by: Reshape.Shape)
      extends PipelineF[A]("$group") {

    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = {
      val Bson.Doc(m) = grouped.bson
      Bson.Doc(m + (Workflow.IdLabel -> by.fold(_.bson, _.cata(bsonƒ))))
    }

    def empty = copy(grouped = Grouped(ListMap()))

    def getAll: List[(BsonField.Leaf, Accumulator)] =
      grouped.value.toList

    def deleteAll(fields: List[BsonField.Leaf]): Workflow.$Group[A] = {
      empty.setAll(getAll.filterNot(t => fields.contains(t._1)))
    }

    def setAll(vs: Seq[(BsonField.Leaf, Accumulator)]) = copy(grouped = Grouped(ListMap(vs: _*)))
  }
  object $Group {
    def make(
      grouped: Grouped, by: Reshape.Shape)(
      src: Workflow):
        Workflow =
      coalesce(Fix($Group(src, grouped, by)))
  }
  val $group = $Group.make _

  final case class $Sort[A](src: A, value: NonEmptyList[(BsonField, SortType)])
      extends ShapePreservingF[A]("$sort") {
    def reparent[B](newSrc: B) = copy(src = newSrc)
    // Note: ListMap preserves the order of entries.
    def rhs = Bson.Doc(ListMap((value.map { case (k, t) => k.asText -> t.bson }).list: _*))
  }
  object $Sort {
    def make(value: NonEmptyList[(BsonField, SortType)])(src: Workflow):
        Workflow =
      coalesce(Fix($Sort(src, value)))
  }
  val $sort = $Sort.make _

  /**
   * TODO: If an $Out has anything after it, we need to either do
   *   $seq($out(src, dst), after($read(dst), ...))
   * or
   *   $Fork(src, List($out(_, dst), after(_, ...)))
   * The latter seems preferable, but currently the forking semantics are not
   * clear.
   */
  final case class $Out[A](src: A, collection: Collection)
      extends ShapePreservingF[A]("$out") {
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = Bson.Text(collection.collectionName)
  }
  object $Out {
    def make(collection: Collection)(src: Workflow): Workflow =
      coalesce(Fix($Out(src, collection)))
  }
  val $out = $Out.make _

  final case class $GeoNear[A](
    src: A,
    near: (Double, Double), distanceField: BsonField,
    limit: Option[Int], maxDistance: Option[Double],
    query: Option[FindQuery], spherical: Option[Boolean],
    distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
    uniqueDocs: Option[Boolean])
      extends PipelineF[A]("$geonear") {
    def reparent[B](newSrc: B) = copy(src = newSrc)
    def rhs = Bson.Doc(List(
      List("near"           -> Bson.Arr(Bson.Dec(near._1) :: Bson.Dec(near._2) :: Nil)),
      List("distanceField"  -> distanceField.bson),
      limit.toList.map(limit => "limit" -> Bson.Int32(limit)),
      maxDistance.toList.map(maxDistance => "maxDistance" -> Bson.Dec(maxDistance)),
      query.toList.map(query => "query" -> query.bson),
      spherical.toList.map(spherical => "spherical" -> Bson.Bool(spherical)),
      distanceMultiplier.toList.map(distanceMultiplier => "distanceMultiplier" -> Bson.Dec(distanceMultiplier)),
      includeLocs.toList.map(includeLocs => "includeLocs" -> includeLocs.bson),
      uniqueDocs.toList.map(uniqueDocs => "uniqueDocs" -> Bson.Bool(uniqueDocs))
    ).flatten.toListMap)
  }
  object $GeoNear {
    def make(
      near: (Double, Double), distanceField: BsonField,
      limit: Option[Int], maxDistance: Option[Double],
      query: Option[FindQuery], spherical: Option[Boolean],
      distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
      uniqueDocs: Option[Boolean])(
      src: Workflow):
        Workflow =
      coalesce(Fix($GeoNear(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)))
  }
  val $geoNear = $GeoNear.make _

  sealed trait MapReduceF[A] extends SingleSourceF[A] {
    def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]): (DocVar, WorkflowTask)
  }

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]$Maps) and the second is the document itself. The function must
    return a 2-element array containing the new key and new value.
    */
  final case class $Map[A](src: A, fn: Js.AnonFunDecl, scope: Scope) extends MapReduceF[A] {
    import $Map._

    def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => compose(this.fn, mapProject(base))
            }),
            $Reduce.reduceNOP,
            selection = sel, inputSort = sort, limit = count, scope = scope)))

    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  object $Map {
    import jscore._

    def make(fn: Js.AnonFunDecl, scope: Scope)(src: Workflow):
        Workflow =
      coalesce(Fix($Map(src, fn, scope)))

    def compose(g: Js.AnonFunDecl, f: Js.AnonFunDecl): Js.AnonFunDecl =
      Js.AnonFunDecl(List("key", "value"), List(
        Js.Return(Js.Call(Js.Select(g, "apply"),
          List(Js.Null, Js.Call(f, List(Js.Ident("key"), Js.Ident("value"))))))))

    def mapProject(base: DocVar) =
      Js.AnonFunDecl(List("key", "value"), List(
        Js.Return(Js.AnonElem(List(Js.Ident("key"), base.toJs(jscore.ident("value")).toJs)))))


    def mapKeyVal(idents: (String, String), key: Js.Expr, value: Js.Expr) =
      Js.AnonFunDecl(List(idents._1, idents._2),
        List(Js.Return(Js.AnonElem(List(key, value)))))
    def mapMap(ident: String, transform: Js.Expr) =
      mapKeyVal(("key", ident), Js.Ident("key"), transform)
    val mapNOP = mapMap("value", Js.Ident("value"))

    def finalizerFn(fn: Js.Expr) =
      Js.AnonFunDecl(List("key", "value"),
        List(Js.Return(Js.Access(
          Js.Call(fn, List(Js.Ident("key"), Js.Ident("value"))),
          Js.Num(1, false)))))

    def mapFn(fn: Js.Expr) =
      Js.AnonFunDecl(Nil,
        List(Js.Call(Js.Select(Js.Ident("emit"), "apply"),
          List(
            Js.Null,
            Js.Call(fn, List(Js.Select(Js.This, IdLabel), Js.This))))))
  }
  val $map = $Map.make _

  // FIXME: this one should become $Map, with the other one being replaced by
  // a new op that combines a map and reduce operation?
  final case class $SimpleMap[A](src: A, exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)
      extends MapReduceF[A] {
    def getAll: Option[List[BsonField]] = {
      def loop(x: JsCore): Option[List[BsonField]] = x match {
        case jscore.Obj(values) => Some(values.toList.flatMap { case (k, v) =>
          val n = BsonField.Name(k.value)
          loop(v).map(_.map(n \ _)).getOrElse(List(n))
        })
        case _ => None
      }
      // Note: this is not safe if `expr` inspects the argument to decide what
      // JS to construct, but all we need here is names of fields that we may
      // be able to optimize away.
      loop(simpleExpr(jscore.ident("?")))
    }

    def deleteAll(fields: List[BsonField]): $SimpleMap[A] = {
      def loop(x: JsCore, fields: List[List[BsonField.Leaf]]): Option[JsCore] = x match {
        case jscore.Obj(values) => Some(jscore.Obj(
          values.collect(Function.unlift[(jscore.Name, JsCore), (jscore.Name, JsCore)] { t =>
            val (k, v) = t
            if (fields contains List(BsonField.Name(k.value))) None
            else {
              val v1 = loop(v, fields.collect {
                case BsonField.Name(k.value) :: tail => tail
              }).getOrElse(v)
              v1 match {
                case jscore.Obj(values) if values.isEmpty => None
                case _ => Some(k -> v1)
              }
            }
          })))
        case _ => Some(x)
      }

      exprs match {
        case NonEmptyList(MapExpr(expr)) =>
          $SimpleMap(src,
            NonEmptyList(
              MapExpr(JsFn(jscore.Name("base"), loop(expr(jscore.ident("base")), fields.map(_.flatten.toList)).getOrElse(jscore.Literal(Js.Null))))),
            scope)
        case _ => this
      }
    }

    private def fn: Js.AnonFunDecl = {
      import jscore._

      def body(fs: List[(CardinalExpr[JsFn], String)]) =
        Js.AnonFunDecl(List("key", "value"),
          List(
            Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
            fs.foldRight[JsCore => Js.Stmt](b =>
              Js.Call(Js.Select(Js.Ident("rez"), "push"),
                List(
                  Js.AnonElem(List(
                    Js.Call(Js.Ident("ObjectId"), Nil),
                    b.toJs))))){
              case ((MapExpr(m), n), inner) => b =>
                Js.Block(List(
                  Js.VarDef(List(n -> m(b).toJs)),
                  inner(ident(n))))
              case ((FlatExpr(m), n), inner) => b =>
                Js.ForIn(Js.Ident("elem"), m(b).toJs,
                  Js.Block(List(
                    Js.VarDef(List(n -> Js.Call(Js.Ident("clone"), List(b.toJs)))),
                    unsafeAssign(m(ident(n)), Access(m(b), ident("elem"))),
                    inner(ident(n)))))
            }(ident("value")),
            Js.Return(Js.Ident("rez"))))

      body(exprs.toList.zipWithIndex.map(("each" + _).second))
    }

    def >>>(that: $SimpleMap[A]) = {
      $SimpleMap(
        this.src,
        (this.exprs.last, that.exprs.head) match {
          case (MapExpr(l), MapExpr(r)) =>
            this.exprs.init <::: NonEmptyList.nel(MapExpr(l >>> r), that.exprs.tail)
          case _ => this.exprs <+> that.exprs
        },
        this.scope <+> that.scope)
    }

    def raw = {
      import jscore._

      val funcs = (exprs).map(_.copoint(ident("_")).para(findFunctionsƒ)).foldLeft(Set[String]())(_ ++ _)

      exprs match {
        case NonEmptyList(MapExpr(expr)) =>
          $Map(src,
            Js.AnonFunDecl(List("key", "value"), List(
              Js.Return(Arr(List(
                ident("key"),
                expr(ident("value")))).toJs))),
            scope <+> $SimpleMap.implicitScope(funcs))
        case _ =>
          $FlatMap(src, fn, $SimpleMap.implicitScope(funcs + "clone") ++ scope)
      }
    }

    def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      raw.newMR(base, src, sel, sort, count)

    def reparent[B](newSrc: B) = copy(src = newSrc)

    def simpleExpr = exprs.foldRight(JsFn.identity) {
      case (MapExpr(expr), acc) => expr >>> acc
      case (_,             acc) => acc
    }
  }
  object $SimpleMap {
    def make(exprs: NonEmptyList[CardinalExpr[JsFn]], scope: Scope)(src: Workflow): Workflow =
      coalesce(Fix($SimpleMap(src, exprs, scope)))

    def implicitScope(fs: Set[String]) =
      $SimpleMap.jsLibrary.filter(x => fs.contains(x._1))

    val jsLibrary = ListMap(
      "remove" -> Bson.JavaScript(
        Js.AnonFunDecl(List("obj", "field"), List(
          Js.VarDef(List("dest" -> Js.AnonObjDecl(Nil))),
          Js.ForIn(Js.Ident("i"), Js.Ident("obj"),
            Js.If(Js.BinOp("!=", Js.Ident("i"), Js.Ident("field")),
              Js.BinOp("=",
                Js.Access(Js.Ident("dest"), Js.Ident("i")),
                Js.Access(Js.Ident("obj"), Js.Ident("i"))),
              None)),
          Js.Return(Js.Ident("dest"))))),
      "clone" -> Bson.JavaScript(
        Js.AnonFunDecl(List("src"), List(
          Js.If(
            Js.BinOp("||",
              Js.BinOp("!=", Js.UnOp("typeof", Js.Ident("src")), Js.Str("object")),
              Js.BinOp("==", Js.Ident("src"), Js.Null)),
            Js.Return(Js.Ident("src")),
            None),
          Js.VarDef(List("dest" -> Js.New(Js.Select(Js.Ident("src"), "constructor")))),
          Js.ForIn(Js.Ident("i"), Js.Ident("src"),
            Js.BinOp ("=",
              Js.Access(Js.Ident("dest"), Js.Ident("i")),
              Js.Call(Js.Ident("clone"), List(
                Js.Access(Js.Ident("src"), Js.Ident("i")))))),
          Js.Return(Js.Ident("dest"))))))
  }
  val $simpleMap = $SimpleMap.make _

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]$Maps) and the second is the document itself. The function must
    return an array of 2-element arrays, each containing a new key and a new
    value.
    */
  final case class $FlatMap[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
      extends MapReduceF[A] {
    import $FlatMap._

    def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => $Map.compose(this.fn, $Map.mapProject(base))
            }),
            $Reduce.reduceNOP,
            selection = sel, inputSort = sort, limit = count, scope = scope)))

    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  object $FlatMap {
    import Js._

    def make(fn: Js.AnonFunDecl, scope: Scope)(src: Workflow):
        Workflow =
      coalesce(Fix($FlatMap(src, fn, scope)))

    private def composition(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      Call(
        Select(Call(f, List(Ident("key"), Ident("value"))), "map"),
        List(AnonFunDecl(List("args"), List(
          Return(Call(Select(g, "apply"), List(Null, Ident("args"))))))))

    def kleisliCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      AnonFunDecl(List("key", "value"), List(
        Return(
          Call(
            Select(Select(AnonElem(Nil), "concat"), "apply"),
            List(AnonElem(Nil), composition(g, f))))))

    def mapCompose(g: Js.AnonFunDecl, f: Js.AnonFunDecl) =
      AnonFunDecl(List("key", "value"), List(Return(composition(g, f))))

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(
          Call(
            Select(
              Call(fn, List(Select(This, IdLabel), This)),
              "map"),
            List(AnonFunDecl(List("__rez"),
              List(Call(Select(Ident("emit"), "apply"),
                List(Null, Ident("__rez")))))))))
  }
  val $flatMap = $FlatMap.make _

  /**
    Takes a function of two parameters – a key and an array of values. The
    function must return a single value.
    */
  final case class $Reduce[A](src: A, fn: Js.AnonFunDecl, scope: Scope)
      extends MapReduceF[A] {
    def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            $Map.mapFn(base match {
              case DocVar(DocVar.ROOT, None) => $Map.mapNOP
              case _                         => $Map.mapProject(base)
            }),
            this.fn,
            selection = sel, inputSort = sort, limit = count, scope = scope)))

    def reparent[B](newSrc: B) = copy(src = newSrc)
  }
  object $Reduce {
    import jscore._

    def make(fn: Js.AnonFunDecl, scope: Scope)(src: Workflow):
        Workflow =
      coalesce(Fix($Reduce(src, fn, scope)))

    val reduceNOP =
      Js.AnonFunDecl(List("key", "values"), List(
        Js.Return(Access(ident("values"), Literal(Js.Num(0, false))).toJs)))

    val reduceFoldLeft =
      Js.AnonFunDecl(List("key", "values"), List(
        Js.VarDef(List("rez" -> Js.AnonObjDecl(Nil))),
        Js.Call(Select(ident("values"), "forEach").toJs,
          List(Js.AnonFunDecl(List("value"),
            List(copyAllFields(ident("value"), Name("rez")))))),
        Js.Return(Js.Ident("rez"))))
  }
  val $reduce = $Reduce.make _

  /**
    Performs a sequence of operations, sequentially, merging their results.
    */
  final case class $FoldLeft[A](head: A, tail: NonEmptyList[A])
      extends WorkflowF[A]
  object $FoldLeft {
    def make(head: Workflow, tail: NonEmptyList[Workflow]):
        Workflow =
      coalesce(Fix($FoldLeft(head, tail)))
  }
  def $foldLeft(first: Workflow, second: Workflow, rest: Workflow*) =
    $FoldLeft.make(first, NonEmptyList.nel(second, rest.toList))

  implicit val WorkflowFRenderTree = new RenderTree[WorkflowF[Unit]] {
    val wfType = "Workflow" :: Nil

    def render(v: WorkflowF[Unit]) = v match {
      case $Pure(value)       => Terminal("$Pure" :: wfType, Some(value.toString))
      case $Read(coll)        => coll.render.copy(nodeType = "$Read" :: wfType)
      case $Match(_, sel)     =>
        NonTerminal("$Match" :: wfType, None, sel.render :: Nil)
      case $Project(_, shape, xId) =>
        NonTerminal("$Project" :: wfType, None,
          Reshape.renderReshape(shape) :+
            Terminal(xId.toString :: "$Project" :: wfType, None))
      case $Redact(_, value) => NonTerminal("$Redact" :: wfType, None,
        value.render ::
          Nil)
      case $Limit(_, count)  => Terminal("$Limit" :: wfType, Some(count.toString))
      case $Skip(_, count)   => Terminal("$Skip" :: wfType, Some(count.toString))
      case $Unwind(_, field) => Terminal("$Unwind" :: wfType, Some(field.toString))
      case $Group(_, grouped, -\/(by)) =>
        val nt = "$Group" :: wfType
        NonTerminal(nt, None,
          grouped.render ::
            NonTerminal("By" :: nt, None, Reshape.renderReshape(by)) ::
            Nil)
      case $Group(_, grouped, \/-(expr)) =>
        val nt = "$Group" :: wfType
        NonTerminal(nt, None,
          grouped.render ::
            Terminal("By" :: nt, Some(expr.toString)) ::
            Nil)
      case $Sort(_, value)   =>
        val nt = "$Sort" :: wfType
        NonTerminal(nt, None,
          value.map { case (field, st) => Terminal("SortKey" :: nt, Some(field.asText + " -> " + st)) }.toList)
      case $GeoNear(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
        val nt = "$GeoNear" :: wfType
        NonTerminal(nt, None,
          Terminal("Near" :: nt, Some(near.toString)) ::
            Terminal("DistanceField" :: nt, Some(distanceField.toString)) ::
            Terminal("Limit" :: nt, Some(limit.toString)) ::
            Terminal("MaxDistance" :: nt, Some(maxDistance.toString)) ::
            Terminal("Query" :: nt, Some(query.toString)) ::
            Terminal("Spherical" :: nt, Some(spherical.toString)) ::
            Terminal("DistanceMultiplier" :: nt, Some(distanceMultiplier.toString)) ::
            Terminal("IncludeLocs" :: nt, Some(includeLocs.toString)) ::
            Terminal("UniqueDocs" :: nt, Some(uniqueDocs.toString)) ::
            Nil)

      case $Map(_, fn, scope) =>
        val nt = "$Map" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $FlatMap(_, fn, scope) =>
        val nt = "$FlatMap" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $SimpleMap(_, exprs, scope) =>
        val nt = "$SimpleMap" :: wfType
        NonTerminal(nt, None,
          exprs.toList.map {
            case MapExpr(e)  => NonTerminal("Map" :: nt, None, List(e.render))
	    case FlatExpr(e) => NonTerminal("Flatten" :: nt, None, List(e.render))          } :+
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)))
      case $Reduce(_, fn, scope) =>
        val nt = "$Reduce" :: wfType
        NonTerminal(nt, None,
          JSRenderTree.render(fn) ::
            Terminal("Scope" :: nt, Some((scope ∘ (_.toJs.pprint(2))).toString)) ::
            Nil)
      case $Out(_, coll) => coll.render.copy(nodeType = "$Out" :: wfType)
      case $FoldLeft(_, _) => Terminal("$FoldLeft" :: wfType, None)
    }
  }

  implicit val WorkflowRenderTree = new RenderTree[Workflow] {
    val wfType = "Workflow" :: Nil

    def chain(op: Workflow): List[RenderedTree] = op.unFix match {
      case ss: SingleSourceF[Workflow] =>
        chain(ss.src) :+ Traverse[WorkflowF].void(ss).render
      case ms => List(render(Fix(ms)))
    }

    def render(v: Workflow) = v.unFix match {
      case op: SourceOp    => op.void.render
      case _: SingleSourceF[Workflow] =>
        NonTerminal("Chain" :: wfType, None, chain(v))
      case $FoldLeft(_, _) =>
        NonTerminal("$FoldLeft" :: wfType, None, v.children.map(render(_)))
    }
  }

  implicit val CrystallizedRenderTree = new RenderTree[Crystallized] {
    def render(v: Crystallized) = v.op.render
  }
}
