package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fs.Path
import slamdata.engine.{Error}
import slamdata.engine.fp._
import optimize.pipeline._
import WorkflowTask._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

// FIXME: FIXPOINT all the things!

/**
 * A WorkflowOp is basically an atomic operation, with references to its inputs.
 * After generating a tree of these (actually, a graph, but we’ll get to that),
 * we crush them down into a Workflow. This `crush` gives us a location to
 * optimize our workflow decisions. EG, A sequence of simple ops may be combined
 * into a single pipeline request, but if one of those operations contains JS,
 * we have to execute that outside of a pipeline, possibly reordering the other
 * operations to avoid having two pipelines with a JS operation in the middle.
 *
 * We should also try to implement the optimizations at
 * http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so
 * that we can build others potentially on top of them (including reordering
 * non-pipelines around pipelines, etc.).
 * 
 * Also, this doesn’t yet go far enough – EG, if we have a ProjectOp before a
 * MapReduceOp, it might make sense to collapse the ProjectOp into the mapping
 * function, but we no longer have a handle on the JS to make that happen.
 */
sealed trait WorkflowOp {
  def srcs: List[WorkflowOp]
  def coalesce: WorkflowOp
  def crush: WorkflowTask

  // TODO: Automatically call `coalesce` when an op is created, rather than here
  //       and recursively in every overriden coalesce.
  def finish: Workflow = {
    Workflow(this.coalesce.deleteUnusedFields(Set.empty).crush)
  }

  def vertices: List[WorkflowOp] = this :: srcs.flatMap(_.vertices)

  import ExprOp.{GroupOp => _, _}
  import PipelineOp._
  import WorkflowOp._

  def rewriteRefs(applyVar0: PartialFunction[DocVar, DocVar]): this.type = {
    val applyVar = (f: DocVar) => applyVar0.lift(f).getOrElse(f)

    def applyExprOp(e: ExprOp): ExprOp = e.mapUp {
      case f : DocVar => applyVar(f)
    }

    def applyFieldName(name: BsonField): BsonField = {
      applyVar(DocField(name)).deref.getOrElse(name) // TODO: Delete field if it's transformed away to nothing???
    }

    def applySelector(s: Selector): Selector = s.mapUpFields(PartialFunction(applyFieldName _))

    def applyReshape(shape: Reshape): Reshape = shape match {
      case Reshape.Doc(value) => Reshape.Doc(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })

      case Reshape.Arr(value) => Reshape.Arr(value.transform {
        case (k, -\/(e)) => -\/(applyExprOp(e))
        case (k, \/-(r)) => \/-(applyReshape(r))
      })
    }

    def applyGrouped(grouped: Grouped): Grouped = Grouped(grouped.value.transform {
      case (k, groupOp) => applyExprOp(groupOp) match {
        case groupOp : ExprOp.GroupOp => groupOp
        case _ => sys.error("Transformation changed the type -- error!")
      }
    })

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

    (this match {
      case ProjectOp(src, shape)     => ProjectOp(src, applyReshape(shape))
      case GroupOp(src, grouped, by) =>
        GroupOp(src,
          applyGrouped(grouped), by.bimap(applyExprOp _, applyReshape _))
      case MatchOp(src, s)           => MatchOp(src, applySelector(s))
      case RedactOp(src, e)          => RedactOp(src, applyExprOp(e))
      case v @ LimitOp(src, _)       => v
      case v @ SkipOp(src, _)        => v
      case v @ UnwindOp(src, f)      => UnwindOp(src, applyVar(f))
      case v @ SortOp(src, l)        => SortOp(src, applyNel(l))
      // case v @ OutOp(src, _)         => v
      case g : GeoNearOp             =>
        g.copy(
          distanceField = applyFieldName(g.distanceField),
          query = g.query.map(applyFindQuery _))
      case v                       => v
    }).asInstanceOf[this.type]
  }

  final def refs: List[DocVar] = {
    // FIXME: Sorry world
    val vf = new scala.collection.mutable.ListBuffer[DocVar]

    rewriteRefs {
      case v => vf += v; v
    }

    vf.toList
  }

  def collectShapes: (List[PipelineOp.Reshape], WorkflowOp) = this match {
    case ProjectOp(src, shape) =>
      Arrow[Function1].first((x: List[PipelineOp.Reshape]) => shape :: x)(src.collectShapes)
    case _                     => (Nil, this)
  }

  def map(f: WorkflowOp => WorkflowOp): WorkflowOp = this match {
    case _: SourceOp => this
    case p: WPipelineOp => p.reparent(f(p.src))
    case _ => sys.error("FIXME: need to handle branching ops")
  }

  def deleteUnusedFields(usedRefs: Set[DocVar]): WorkflowOp = {
    def getRefs(op: WorkflowOp): Set[DocVar] = (op match {
       // Don't count unwinds (if the var isn't referenced elsewhere, it's effectively unused)
      case UnwindOp(_, _) => Nil
      case _ => op.refs
    }).toSet

    def unused(defs: Set[DocVar], refs: Set[DocVar]): Set[DocVar] = {
      defs.filterNot(d => refs.exists(ref => d.startsWith(ref) || ref.startsWith(d)))
    }

    def getDefs(op: WorkflowOp): Set[DocVar] = (op match {
      case p @ ProjectOp(_, _) => p.getAll.map(_._1)
      case g @ GroupOp(_, _, _) => g.getAll.map(_._1)
      case _ => Nil
    }).map(DocVar.ROOT(_)).toSet


    val pruned = if (!usedRefs.isEmpty) {
      val unusedRefs =
        unused(getDefs(this), usedRefs).toList.flatMap(_.deref.toList)
      this match {
        case p @ ProjectOp(_, _) => p.deleteAll(unusedRefs)
        case g @ GroupOp(_, _, _) => g.deleteAll(unusedRefs.map(_.flatten.head))
        case _ => this
      }
    }
      else this
    pruned.map(_.deleteUnusedFields(usedRefs ++ getRefs(pruned)))
  }
}

object WorkflowOp {
  // implicit val WorkflowOpTraverse = new Traverse[WorkflowOp] {
  //   def traverseImpl[G[_], A, B](fa: WorkflowOp[A])(f: A => G[B])
  //     (implicit G: Applicative[G]):
  //       G[WorkflowOp[B]] = fa match {
  //     case x @ PureOp(_) => G.point(x)
  //     case x @ ReadOp(_) => G.point(x)
  //     case MatchOp(src, sel) => G.apply(f(src))(MatchOp(_, sel))
  //     case ProjectOp(src, shape) => G.apply(f(src))(ProjectOp(_, shape))
  //     case RedactOp(src, value) => G.apply(f(src))(RedactOp(_, value))
  //     case LimitOp(src, count) => G.apply(f(src))(LimitOp(_, count))
  //     case SkipOp(src, count) => G.apply(f(src))(SkipOp(_, count))
  //     case UnwindOp(src, field) => G.apply(f(src))(UnwindOp(_, field))
  //     case GroupOp(src, grouped, by) => G.apply(f(src))(GroupOp(_, grouped, by))
  //     case SortOp(src, value) => G.apply(f(src))(SortOp(_, value))
  //     case GeoNearOp(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs) =>
  //       G.apply(f(src))(GeoNearOp(_, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs))
  //     case MapReduceOp(src, mr) => G.apply(f(src))(MapReduceOp(_, mr))
  //     case FoldLeftOp(srcs) => G.map(Traverse[NonEmptyList].sequence(srcs.map(f)))(FoldLeftOp(_))
  //     case JoinOp(srcs) => G.map(Traverse[NonEmptyList].sequence(srcs.map(f)))(JoinOp(_))
  //     // case OutOp(src, col) => G.apply(f(src))(OutOp(_, col))
  //   }
  // }

  /**
   * Operations without an input.
   */
  sealed trait SourceOp extends WorkflowOp {
    def coalesce = this
    def srcs = Nil // Set.empty
  }

  /**
   * This should be renamed once the other PipelineOp goes away, but it is the
   * subset of operations that can ever be pipelined.
   */
  sealed trait WPipelineOp extends WorkflowOp {
    def src: WorkflowOp
    def pipeline: Option[(WorkflowTask, List[PipelineOp])]
    def reparent(newSrc: WorkflowOp): WPipelineOp

    def coalesce: WorkflowOp = reparent(src.coalesce)
    def srcs = List(src) // Set(src)
  }
  sealed trait ShapePreservingOp extends WPipelineOp

  /**
   * Flattens the sequence of operations like so:
   * 
   *   chain(
   *     ReadOp(Path.fileAbs("foo")),
   *     MatchOp(_, Selector.Where(Js.Bool(true))),
   *     LimitOp(_, 7))
   * ==
   *   LimitOp(
   *     MatchOp(
   *       ReadOp(Path.fileAbs("foo")),
   *       Selector.Where(Js.Bool(true))),
   *     7)
   */
  def chain(src: WorkflowOp, ops: (WorkflowOp => WorkflowOp)*): WorkflowOp =
    ops.foldLeft(src)((s, o) => o(s))

  /**
   * A dummy op does nothing except complete a WorkflowOp so it can be merged
   * as the tail of some other op.
   */
  // FIXME: get rid of this
  case object DummyOp extends SourceOp {
    def crush = sys.error("Should not have any DummyOps at this point.")
  }

  case class PureOp(value: Bson) extends SourceOp {
    def crush = PureTask(value)
  }

  case class ReadOp(path: Path) extends SourceOp {
    def crush = WorkflowTask.ReadTask(Collection(path.filename))
  }

  case class MatchOp(src: WorkflowOp, selector: Selector) extends ShapePreservingOp {
    override def coalesce = src.coalesce match {
      case SortOp(src0, value) =>
        SortOp(MatchOp(src0, selector), value).coalesce
      case MatchOp(src0, sel0) =>
        MatchOp(src0, Semigroup[Selector].append(sel0, selector)).coalesce
      case csrc => reparent(csrc)
    }
    def crush = {
      // TODO: If we ever allow explicit request of cursors (instead of
      //       collections), we could generate a FindQuery here.
      lazy val nonPipeline =
        MapReduceTask(
          src.crush,
          MapReduce(
            Js.AnonFunDecl(Nil,
              List(Js.Call(Js.Ident("emit"),
                List(Js.Select(Js.Ident("this"), "_id"), Js.Ident("this"))))),
            Js.AnonFunDecl(List("key", "values"),
              List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(0, false))))),
            selection = Some(selector)))
      pipeline match {
        // TODO: incorporate `simplify` into the `coalesce` here
        case Some((up, mine)) => PipelineTask(up, Pipeline(mine))
        case None             => nonPipeline
      }
    }
    def pipeline = {
      def pipelinable(sel: Selector): Boolean = sel match {
        case Selector.Where(_) => false
        case comp: Selector.CompoundSelector =>
          pipelinable(comp.left) && pipelinable(comp.right)
        case _ => true
      }
      if (pipelinable(selector)) {
        val op = PipelineOp.Match(selector)
        src match {
          case p: WPipelineOp => p.pipeline.cata(
            { case (up, prev) => Some((up, prev :+ op)) },
            Some((src.crush, List(op))))
          case _ => Some((src.crush, List(op)))
        }
      }
      else None
    }
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }

  private def alwaysPipePipe(src: WorkflowOp, op: PipelineOp) = {
    src match {
      case p: WPipelineOp => p.pipeline.cata(
        { case (up, prev) => (up, prev :+ op) },
        (src.crush, List(op)))
      case _ => (src.crush, List(op))
    }
  }

  private def alwaysCrushPipe(src: WorkflowOp, op: PipelineOp) =
    alwaysPipePipe(src, op) match {
      // TODO: incorporate `simplify` into the `coalesce` here
      case (up, pipe) => PipelineTask(up, Pipeline(pipe))
    }

  case class ProjectOp(src: WorkflowOp, shape: PipelineOp.Reshape)
      extends WPipelineOp {

    import PipelineOp._

    private def pipeop = PipelineOp.Project(shape)
    override def coalesce = src.coalesce match {
      case ProjectOp(_, _) =>
        val (rs, src) = this.collectShapes
        inlineProject(rs.head, rs.tail).map(ProjectOp(src, _).coalesce).getOrElse(this)
      case csrc => reparent(csrc)
    }
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)

    def empty: ProjectOp = shape match {
      case Reshape.Doc(_) => ProjectOp.EmptyDoc(src)
      case Reshape.Arr(_) => ProjectOp.EmptyArr(src)
    }

    def set(field: BsonField, value: ExprOp \/ PipelineOp.Reshape): ProjectOp =
      ProjectOp(src, shape.set(field, value))

    def getAll: List[(BsonField, ExprOp)] = {
      def fromReshape(r: Reshape): List[(BsonField, ExprOp)] = r match {
        case Reshape.Arr(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
        case Reshape.Doc(m) => m.toList.map { case (f, e) => getAll0(f, e) }.flatten
      }

      def getAll0(f0: BsonField, e: ExprOp \/ Reshape): List[(BsonField, ExprOp)] = e.fold(
        e => (f0 -> e) :: Nil,
        r => fromReshape(r).map { case (f, e) => (f0 \ f) -> e }
      )

      fromReshape(shape)
    }

    def setAll(fvs: Iterable[(BsonField, ExprOp \/ PipelineOp.Reshape)]): ProjectOp = fvs.foldLeft(this) {
      case (project, (field, value)) => project.set(field, value)
    }

    def deleteAll(fields: List[BsonField]): ProjectOp = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1.startsWith(_))).map(t => t._1 -> -\/ (t._2)))
    }
  }
  object ProjectOp {
    import PipelineOp._

    val EmptyDoc = (src: WorkflowOp) => ProjectOp(src, Reshape.EmptyDoc)
    val EmptyArr = (src: WorkflowOp) => ProjectOp(src, Reshape.EmptyArr)   
  }

  case class RedactOp(src: WorkflowOp, value: ExprOp) extends WPipelineOp {
    private def pipeop = PipelineOp.Redact(value)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }

  case class LimitOp(src: WorkflowOp, count: Long) extends ShapePreservingOp {
    import MapReduce._

    private def pipeop = PipelineOp.Limit(count)
    override def coalesce = src.coalesce match {
      case LimitOp(src0, count0) =>
        LimitOp(src0, Math.min(count0, count)).coalesce
      case MapReduceOp(src0, mr) =>
        MapReduceOp(
          src0,
          mr applyLens _limit modify (x => Some(x match {
            case None        => count
            case Some(count0) => Math.min(count, count0)}))).coalesce
      case SkipOp(src0, count0) =>
        SkipOp(LimitOp(src0, count0 + count), count).coalesce
      case csrc => reparent(csrc)
    }
    // TODO: If the preceding is a MatchOp, and it or its source isn’t
    //       pipelineable, then return a FindQuery combining the match and this
    //       limit
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  case class SkipOp(src: WorkflowOp, count: Long) extends ShapePreservingOp {
    private def pipeop = PipelineOp.Skip(count)
    override def coalesce = src.coalesce match {
      case SkipOp(src0, count0) => SkipOp(src0, count0 + count).coalesce
      case csrc                 => reparent(csrc)
    }
    // TODO: If the preceding is a MatchOp (or a limit preceded by a MatchOp),
    //       and it or its source isn’t pipelineable, then return a FindQuery
    //       combining the match and this skip
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  case class UnwindOp(src: WorkflowOp, field: ExprOp.DocVar)
      extends WPipelineOp {
    private def pipeop = PipelineOp.Unwind(field)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  case class GroupOp(
    src: WorkflowOp,
    grouped: PipelineOp.Grouped,
    by: ExprOp \/ PipelineOp.Reshape)
      extends WPipelineOp {

    import PipelineOp._

    // TODO: Not all GroupOps can be pipelined. Need to determine when we may
    //       need the group command or a map/reduce.
    private def pipeop = PipelineOp.Group(grouped, by)
    override def coalesce = {
      val reparented = this.reparent(this.src.coalesce)
      reparented match {
        case g @ GroupOp(_, _, _) =>
          inlineGroupProjects(g).getOrElse(reparented)
        case _ => reparented
      }
    }
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)

    def toProject: ProjectOp = grouped.value.foldLeft(ProjectOp(src, PipelineOp.Reshape.EmptyArr)){
      case (p, (f, v)) => p.set(f, -\/ (v))
    }

    def empty = copy(grouped = Grouped(ListMap()))

    def getAll: List[(BsonField.Leaf, ExprOp.GroupOp)] =
      grouped.value.toList

    def deleteAll(fields: List[BsonField.Leaf]): WorkflowOp.GroupOp = {
      empty.setAll(getAll.filterNot(t => fields.exists(t._1 == _)))
    }

    def setAll(vs: Seq[(BsonField.Leaf, ExprOp.GroupOp)]) = copy(grouped = Grouped(ListMap(vs: _*)))
  }

  case class SortOp(src: WorkflowOp, value: NonEmptyList[(BsonField, SortType)])
      extends WPipelineOp {
    private def pipeop = PipelineOp.Sort(value)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }

  /**
   * TODO: If an OutOp has anything after it, we need to either do
   *   SeqOp(OutOp(src, dst), after(ReadOp(dst), ...))
   * or
   *   ForkOp(src, List(OutOp(_, dst), after(_, ...)))
   * The latter seems preferable, but currently the forking semantics are not
   * clear.
   */
  // case class OutOp(src: WorkflowOp, collection: Collection) extends ShapePreservingOp {
  //   def coalesce = src.coalesce match {
  //     case read @ ReadOp(_) => read
  //     case _                => this
  //   }
  //   def pipeline = Some(alwaysPipePipe(src, PipelineOp.Out(field)))
  // }

  case class GeoNearOp(src: WorkflowOp,
                       near: (Double, Double), distanceField: BsonField,
                       limit: Option[Int], maxDistance: Option[Double],
                       query: Option[FindQuery], spherical: Option[Boolean],
                       distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
                       uniqueDocs: Option[Boolean])
      extends WPipelineOp {
    private def pipeop = PipelineOp.GeoNear(near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)
    override def coalesce = src.coalesce match {
      case _: GeoNearOp   => this
      case p: WPipelineOp => p.reparent(copy(src = p.src)).coalesce
      case csrc           => reparent(csrc)
    }
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }

  case class MapReduceOp(src: WorkflowOp, mr: MapReduce) extends WorkflowOp {
    import MapReduce._

    def srcs = List(src) // Set(src)
    def coalesce = src.coalesce match {
      case MatchOp(src0, sel0) =>
        MapReduceOp(src0,
          mr applyLens _selection modify (x => Some(x match {
            case None      => sel0
            case Some(sel) => Semigroup[Selector].append(sel0, sel)
          }))).coalesce
      case SortOp(src0, keys0) =>
        MapReduceOp(src0,
          mr applyLens _inputSort modify (x => Some(x match {
            case None       => keys0
            case Some(keys) => keys append keys0
          }))).coalesce
      case _ => this
    }
    def crush = MapReduceTask(src.crush, mr)
  }

  case class FoldLeftOp(lsrcs: NonEmptyList[WorkflowOp]) extends WorkflowOp {
    def srcs = lsrcs.toList
    def coalesce = FoldLeftOp(lsrcs.map(_.coalesce))
    def crush = FoldLeftTask(lsrcs.map(_.crush))
  }

  case class JoinOp(ssrcs: Set[WorkflowOp]) extends WorkflowOp {
    def srcs = ssrcs.toList
    def coalesce = JoinOp(ssrcs.map(_.coalesce))
    def crush = JoinTask(ssrcs.map(_.crush))
  }
}
