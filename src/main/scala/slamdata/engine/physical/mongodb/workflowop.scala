package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fs.Path
import slamdata.engine.{Error, RenderTree, Terminal, NonTerminal}
import slamdata.engine.fp._
import optimize.pipeline._
import WorkflowTask._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

// FIXME: FIXPOINT all the things!

/**
  A WorkflowOp is basically an atomic operation, with references to its inputs.
  After generating a tree of these (actually, a graph, but we’ll get to that),
  we crush them down into a Workflow. This `crush` gives us a location to
  optimize our workflow decisions. EG, A sequence of simple ops may be combined
  into a single pipeline request, but if one of those operations contains JS, we
  have to execute that outside of a pipeline, possibly reordering the other
  operations to avoid having two pipelines with a JS operation in the middle.
 
  We also implement the optimizations at
  http://docs.mongodb.org/manual/core/aggregation-pipeline-optimization/ so that
  we can build others potentially on top of them (including reordering
  non-pipelines around pipelines, etc.).
  */
sealed trait WorkflowOp {
  import ExprOp.DocVar
  import WorkflowBuilder.ExprVar

  def srcs: List[WorkflowOp]
  def coalesce: WorkflowOp
  /**
    Returns both the final WorkflowTask as well as a DocVar indicating the base
    of the collection.
    */
  def crush: (DocVar, WorkflowTask)

  // TODO: Automatically call `coalesce` when an op is created, rather than here
  //       and recursively in every overridden coalesce.
  def finish: WorkflowOp = this.coalesce.deleteUnusedFields(Set.empty)

  def workflow: Workflow = Workflow(finalize(this.finish).crush._2)
  

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
    case p: WPipelineOp     => p.reparent(f(p.src))
    case MapOp(src, fn)     => MapOp(f(src), fn)
    case FlatMapOp(src, fn) => FlatMapOp(f(src), fn)
    case ReduceOp(src, fn)  => ReduceOp(f(src), fn)
    case FoldLeftOp(srcs)   => FoldLeftOp(srcs.map(f))
    case FoldLeftOp0(srcs)   => FoldLeftOp0(srcs.map(f))
    case JoinOp(srcs)       => JoinOp(srcs.map(f))
    // case OutOp(src, dst)    => OutOp(f(src), dst)
  }

  def deleteUnusedFields(usedRefs: Set[DocVar]): WorkflowOp = {
    def getRefs[A](op: WorkflowOp, prev: Set[DocVar]): Set[DocVar] =
      (op match {
        // Don't count unwinds (if the var isn't referenced elsewhere, it's effectively unused)
        case UnwindOp(_, _) => prev
        case WorkflowOp.GroupOp(_, _, _) => op.refs
        // FIXME: Since we can’t reliably identify which fields are used by a JS
        //        function, we need to assume they all are, until we hit the
        //        next GroupOp or ProjectOp.
        case MapOp(_, _) => Nil
        case FlatMapOp(_, _) => Nil
        case ReduceOp(_, _) => Nil
        case ProjectOp(_, _) => op.refs
        case _ => prev ++ op.refs
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
    pruned.map(_.deleteUnusedFields(getRefs(pruned, usedRefs)))
  }

  /**
    Performs some irreversible conversions, meant to be used once, after the
    entire workflow has been generated.
    */
  // probable conversions
  // to MapOp:          ProjectOp
  // to FlatMapOp:      MatchOp, LimitOp (using scope), SkipOp (using scope), UnwindOp, GeoNearOp
  // to MapOp/ReduceOp: GroupOp
  // ???:               RedactOp
  // none:              SortOp
  // NB: We don’t convert a ProjectOp after a map/reduce op because it could
  //     affect the final shape unnecessarily.
  private def finalize(op: WorkflowOp): WorkflowOp = op.coalesce match {
    case FlatMapOp(ProjectOp(src, shape), fn) =>
      shape.toJs(Js.Ident("value")).fold(op.map(finalize(_)))(
        x => finalize(FlatMapOp(MapOp(finalize(src), MapOp.mapMap("value", x)), fn)))
    case FlatMapOp(uw @ UnwindOp(src, _), fn) =>
      finalize(FlatMapOp(FlatMapOp(finalize(src), uw.flatmapop), fn))
    case MapOp(ProjectOp(src, shape), fn) =>
      shape.toJs(Js.Ident("value")).fold(op.map(finalize(_)))(
        x => finalize(MapOp(MapOp(finalize(src), MapOp.mapMap("value", x)), fn)))
    case MapOp(uw @ UnwindOp(src, _), fn) =>
      finalize(MapOp(FlatMapOp(finalize(src), uw.flatmapop), fn))
    case ReduceOp(ProjectOp(src, shape), fn) =>
      shape.toJs(Js.Ident("value")).fold(op.map(finalize(_)))(
        x => finalize(ReduceOp(MapOp(finalize(src), MapOp.mapMap("value", x)), fn)))
    case ReduceOp(uw @ UnwindOp(src, _), fn) =>
      finalize(ReduceOp(FlatMapOp(finalize(src), uw.flatmapop), fn))
    case op @ FoldLeftOp(lsrcs) =>
      FoldLeftOp0(NonEmptyList.nel(
        finalize(ProjectOp(lsrcs.head,
          PipelineOp.Reshape.Doc(ListMap(
            WorkflowBuilder.ExprName -> -\/(ExprOp.DocVar.ROOT()))))),
        lsrcs.tail.map(x => finalize(x match {
          case op @ ReduceOp(_, _) => op
          case op => ReduceOp(op, ReduceOp.reduceFoldLeft)
        }))))
      

    case op => op.map(finalize(_))
  }
}

object WorkflowOp {
  import ExprOp.DocVar
  import WorkflowBuilder.ExprVar

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
    def pipeline: Option[(DocVar, WorkflowTask, List[PipelineOp])]
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

  case class PureOp(value: Bson) extends SourceOp {
    def crush = (DocVar.ROOT(),  PureTask(value))
  }

  case class ReadOp(coll: Collection) extends SourceOp {
    def crush = (DocVar.ROOT(), ReadTask(coll))
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
      lazy val nonPipeline = {
        val (base, crushed) = src.crush
        (ExprVar,
          MapReduceTask(
            crushed,
            MapReduce(
              MapOp.mapFn(base match {
                case DocVar(DocVar.ROOT, None) => MapOp.mapNOP
                case _ => MapOp.mapProject(base)
              }),
              ReduceOp.reduceNOP,
              selection = Some(PipelineOp.Match(selector).rewriteRefs(PartialFunction(base \\ _)).selector))))
      }
      pipeline match {
        case Some((base, up, mine)) =>
          (base, PipelineTask(up, Pipeline(mine)))
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
        lazy val (base, crushed) = src.crush
        val op = PipelineOp.Match(selector)
        src match {
          case p: WPipelineOp => p.pipeline.cata(
            { case (base, up, prev) => Some((base, up, prev :+ op.rewriteRefs(PartialFunction(base \\ _)))) },

            Some((base, crushed, List(op.rewriteRefs(PartialFunction(base \\ _))))))
          case _ => Some((base, crushed, List(op.rewriteRefs(PartialFunction(base \\ _)))))
        }
      }
      else None
    }
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }

  private def alwaysPipePipe(src: WorkflowOp, op: PipelineOp) = {
    lazy val (base, crushed) = src.crush
    // TODO: this is duplicated in `WorkflowBuilder.rewrite`
    def repairBase(base: DocVar) = op match {
      case PipelineOp.Group(_, _) => DocVar.ROOT()
      case PipelineOp.Project(_)  => DocVar.ROOT()
      case _                      => base
    }
    src match {
      case p: WPipelineOp => p.pipeline.cata(
        {
          case (base, up, prev) =>
            (repairBase(base),
              up,
              prev :+ op.rewriteRefs(PartialFunction(base \\ _)))
        },
        (repairBase(base),
          crushed,
          List(op.rewriteRefs(PartialFunction(base \\ _)))))
      case _ =>
        (repairBase(base),
          crushed,
          List(op.rewriteRefs(PartialFunction(base \\ _))))
    }
  }

  private def alwaysCrushPipe(src: WorkflowOp, op: PipelineOp) =
    alwaysPipePipe(src, op) match {
      case (base, up, pipe) => (base, PipelineTask(up, Pipeline(pipe)))
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
      case SkipOp(src0, count0) =>
        SkipOp(LimitOp(src0, count0 + count), count0).coalesce
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
    lazy val flatmapop = {
      val feld = field.toJs(Js.Ident("value"))
      Js.AnonFunDecl(List("key", "value"),
        List(
          Js.VarDef(List("each" -> Js.AnonObjDecl(Nil))),
          ReduceOp.copyAllFields(Js.Ident("value"))(Js.Ident("each")),
          Js.Return(Js.Call(Js.Select(feld, "map"), List(
            Js.AnonFunDecl(List("elem"), List(
              Js.BinOp("=", field.toJs(Js.Ident("each")), Js.Ident("elem")),
              Js.Return(
                Js.AnonElem(List(
                  Js.Call(Js.Ident("ObjectId"), Nil),
                  Js.Ident("each")))))))))))
    }
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

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]MapOps) and the second is the document itself. The function must
    return a 2-element array containing the new key and new value.
    */
  case class MapOp(src: WorkflowOp, fn: Js.AnonFunDecl) extends WorkflowOp {
    import MapOp._
    import Js._

    def srcs = List(src)
    def coalesce = src.coalesce match {
      case MapOp(src0, fn0)     => MapOp(src0, compose(fn, fn0))
      case FlatMapOp(src0, fn0) =>
        FlatMapOp(src0, FlatMapOp.compose(fn, fn0, false))
      case csrc                 => MapOp(csrc, fn)
    }

    private def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) = 
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => compose(this.fn, mapProject(base))
            }),
            ReduceOp.reduceNOP,
            selection = sel, inputSort = sort, limit = count)))

    def crush = {
      val (base, crushed) = src.crush
      crushed match {
        case MapReduceTask(src0, mr @ MapReduce(_, _, _, _, _, _, None, _, _, _)) =>
          (base, MapReduceTask(src0, mr applyLens MapReduce._finalizer set Some(finalizerFn(this.fn))))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
          newMR(base, src0, Some(sel), None, None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
          newMR(base, src0, None, Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
          newMR(base, src0, Some(sel), Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, Some(sort), Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), Some(sort), Some(count))
        case srcTask =>
          newMR(base, srcTask, None, None, None)
      }
    }
  }
  object MapOp {
    import Js._

    def compose(g: Js.AnonFunDecl, f: Js.AnonFunDecl): Js.AnonFunDecl =
      AnonFunDecl(List("key", "value"), List(
        Return(Call(Select(g, "apply"),
          List(Null, Call(f, List(Ident("key"), Ident("value"))))))))

    def mapProject(base: DocVar) =
      AnonFunDecl(List("key", "value"), List(
        Return(AnonElem(List(Ident("key"), base.toJs(Ident("value")))))))


    def mapKeyVal(idents: (String, String), key: Js.Expr, value: Js.Expr) =
      AnonFunDecl(List(idents._1, idents._2),
        List(Return(AnonElem(List(key, value)))))
    def mapMap(ident: String, transform: Js.Expr) =
      mapKeyVal(("key", ident), Ident("key"), transform)
    val mapNOP = mapMap("value", Ident("value"))

    def finalizerFn(fn: Js.Expr) =
      AnonFunDecl(List("key", "value"),
        List(Return(Access(
          Call(fn, List(Ident("key"), Ident("value"))),
          Num(1, false)))))

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(Call(Select(Ident("emit"), "apply"),
          List(
            Null,
            Call(fn, List(Select(This, "_id"), This))))))
  }

  /**
    Takes a function of two parameters. The first is the current key (which
    defaults to `this._id`, but may have been overridden by previous
    [Flat]MapOps) and the second is the document itself. The function must
    return an array of 2-element arrays, each containing a new key and a new
    value.
    */
  case class FlatMapOp(src: WorkflowOp, fn: Js.AnonFunDecl) extends WorkflowOp {
    import FlatMapOp._
    import Js._

    def srcs = List(src)
    def coalesce = src.coalesce match {
      case MapOp(src0, fn0)     => FlatMapOp(src0, MapOp.compose(fn, fn0))
      case FlatMapOp(src0, fn0) =>
        FlatMapOp(src0, FlatMapOp.compose(fn, fn0, true))
      case csrc                 => FlatMapOp(csrc, fn)
    }

    private def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            mapFn(base match {
              case DocVar(DocVar.ROOT, None) => this.fn
              case _ => MapOp.compose(this.fn, MapOp.mapProject(base))
            }),
            ReduceOp.reduceNOP,
            selection = sel, inputSort = sort, limit = count)))
    def crush = {
      val (base, crushed) = src.crush
      crushed match {
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
          newMR(base, src0, Some(sel), None, None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
          newMR(base, src0, None, Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
          newMR(base, src0, Some(sel), Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, Some(sort), Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), Some(sort), Some(count))
        case srcTask =>
          newMR(base, srcTask, None, None, None)
      }
    }
  }
  object FlatMapOp {
    import Js._

    def compose(g: Js.AnonFunDecl, f: Js.AnonFunDecl, shouldConcat: Boolean) = {
      val composition =
        Call(
          Select(Call(f, List(Ident("key"), Ident("value"))), "map"),
          List(AnonFunDecl(List("args"), List(
            Return(Call(Select(g, "apply"), List(Null, Ident("args"))))))))
      AnonFunDecl(List("key", "value"), List(
        Return(
          if (shouldConcat)
            Call(
              Select(Select(AnonElem(Nil), "concat"), "apply"),
              List(AnonElem(Nil), composition))
          else composition)))
    }

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(
          Call(
            Select(
              Call(fn, List(Select(This, "_id"), This)),
              "map"),
            List(AnonFunDecl(List("__rez"),
              List(Call(Select(Ident("emit"), "apply"),
                List(Null, Ident("__rez")))))))))
  }

  /**
    Takes a function of two parameters – a key and an array of values. The
    function must return a single value.
    */
  case class ReduceOp(src: WorkflowOp, fn: Js.AnonFunDecl) extends WorkflowOp {
    import ReduceOp._

    def srcs = List(src)
    def coalesce = ReduceOp(src.coalesce, fn)

    private def newMR(base: DocVar, src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      (ExprVar,
        MapReduceTask(src,
          MapReduce(
            MapOp.mapFn(base match {
              case DocVar(DocVar.ROOT, None) => MapOp.mapNOP
              case _                         => MapOp.mapProject(base)
            }),
            this.fn,
            selection = sel, inputSort = sort, limit = count)))

    def crush = {
      val (base, crushed) = src.crush
      crushed match {
        case MapReduceTask(src0, mr @ MapReduce(_, reduceNOP, _, _, _, _, None, _, _, _)) =>
          (base, MapReduceTask(src0, mr applyLens MapReduce._reduce set this.fn))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
          newMR(base, src0, Some(sel), None, None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
          newMR(base, src0, None, Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
          newMR(base, src0, Some(sel), Some(sort), None)
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), None, Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, None, Some(sort), Some(count))
        case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
          newMR(base, src0, Some(sel), Some(sort), Some(count))
        case srcTask =>
          newMR(base, srcTask, None, None, None)
      }
    }
  }
  object ReduceOp {
    import Js._

    val reduceNOP =
      AnonFunDecl(List("key", "values"), List(
        Return(Access(Ident("values"), Num(0, false)))))

    def copyOneField(key: Js.Expr => Js.Expr, expr: Js.Expr):
        Js.Expr => Js.Stmt =
      base => Js.BinOp("=", key(base), expr)

    def copyAllFields(expr: Js.Expr): Js.Expr => Js.Stmt = base =>
      Js.ForIn(Js.Ident("attr"), expr,
        Js.If(
          Js.Call(Js.Select(expr, "hasOwnProperty"), List(Js.Ident("attr"))),
          copyOneField(
            Js.Access(_, Js.Ident("attr")),
            Js.Access(expr, Js.Ident("attr")))(base),
          None))

    val reduceFoldLeft =
      AnonFunDecl(List("key", "values"), List(
        VarDef(List("rez" -> AnonObjDecl(Nil))),
        Call(Select(Ident("values"), "forEach"),
          List(AnonFunDecl(List("value"),
            List(copyAllFields(Ident("value"))(Ident("rez")))))),
        Return(Ident("rez"))))
  }

  /**
    Performs a sequence of operations, sequentially, merging their results.
    */
  case class FoldLeftOp(lsrcs: NonEmptyList[WorkflowOp]) extends WorkflowOp {
    def srcs = lsrcs.toList
    def coalesce = lsrcs.map(_.coalesce) match {
      case NEL(src,                Nil)  => src
      case NEL(FoldLeftOp(csrcs0), tail) => FoldLeftOp(csrcs0 :::> tail)
      case csrcs                         => FoldLeftOp(csrcs)
    }
    def crush = sys.error("Trying to crush un-patched FoldLeftOp.")
  }

  /**
    This exists solely to prevent repeated insertion of ops when we traverse
    FoldLeftOp multiple times in `finalize`. A better `finalize` might obviate this.
    */
  private case class FoldLeftOp0(lsrcs: NonEmptyList[WorkflowOp])
      extends WorkflowOp {
    def srcs = lsrcs.toList
    def coalesce = lsrcs.map(_.coalesce) match {
      case NEL(src,                Nil)  => src
      case NEL(FoldLeftOp(csrcs0), tail) => FoldLeftOp0(csrcs0 :::> tail)
      case csrcs                         => FoldLeftOp0(csrcs)
    }
    def crush =
      (ExprVar,
        FoldLeftTask(
          lsrcs.head.crush._2,
          lsrcs.tail.map(_.crush._2 match {
            case MapReduceTask(src, mr) =>
              // FIXME: FoldLeftOp currently always reduces, but in future we’ll
              //        want to have more control.
              MapReduceTask(src,
                mr applyLens MapReduce._out set Some(MapReduce.WithAction(MapReduce.Action.Reduce)))
            // NB: `finalize` should ensure that the final op is always a
            //     ReduceOp.
            case src => sys.error("not a mapReduce: " + src)
          }) match {
            case head :: tail => NonEmptyList.nel(head, tail)
            // NB: `coalesce` should ensure that we always have at least two
            //     srcs.
            case Nil          => sys.error("can’t fold a single task")
          }))
  }

  case class JoinOp(ssrcs: Set[WorkflowOp]) extends WorkflowOp {
    def srcs = ssrcs.toList
    def coalesce = JoinOp(ssrcs.map(_.coalesce))
    def crush = (ExprVar, JoinTask(ssrcs.map(_.crush._2)))
  }
  
  implicit def WorkflowOpRenderTree(implicit RS: RenderTree[Selector], RE: RenderTree[ExprOp], RG: RenderTree[PipelineOp.Grouped]): RenderTree[WorkflowOp] = new RenderTree[WorkflowOp] {
    def nodeType(subType: String) = "WorkflowOp" :: subType :: Nil
    
    def render(v: WorkflowOp) = v match {
      case PureOp(value)        => Terminal(value.toString, nodeType("PureOp"))
      case ReadOp(coll)         => Terminal(coll.name, nodeType("ReadOp"))
      case MatchOp(src, sel)    => NonTerminal("", 
                                      WorkflowOpRenderTree.render(src) :: 
                                      RS.render(sel) ::
                                      Nil,
                                    nodeType("MatchOp"))
      case ProjectOp(src, shape) => NonTerminal("",
                                      WorkflowOpRenderTree.render(src) :: 
                                        PipelineOp.renderReshape("Fields", "", shape) ::
                                        Nil,
                                      nodeType("ProjectOp"))
      case RedactOp(src, value) => NonTerminal("", 
                                      WorkflowOpRenderTree.render(src) :: 
                                      RE.render(value) ::
                                      Nil,
                                    nodeType("RedactOp"))
      case LimitOp(src, count)  => NonTerminal(count.toString, WorkflowOpRenderTree.render(src) :: Nil, nodeType("LimitOp"))
      case SkipOp(src, count)   => NonTerminal(count.toString, WorkflowOpRenderTree.render(src) :: Nil, nodeType("SkipOp"))
      case UnwindOp(src, field) => NonTerminal(field.toString, WorkflowOpRenderTree.render(src) :: Nil, nodeType("UnwindOp"))
      case GroupOp(src, grouped, -\/ (expr)) => NonTerminal("",
                                      WorkflowOpRenderTree.render(src) ::
                                        RG.render(grouped) ::
                                        Terminal(expr.toString, nodeType("By")) ::
                                        Nil,
                                      nodeType("GroupOp"))
      case GroupOp(src, grouped, \/- (by)) => NonTerminal("",
                                      WorkflowOpRenderTree.render(src) ::
                                        RG.render(grouped) ::
                                        PipelineOp.renderReshape("By", "", by) ::
                                        Nil,
                                      nodeType("GroupOp"))
      case SortOp(src, value)   => NonTerminal("",
                                    WorkflowOpRenderTree.render(src) ::
                                      value.map { case (field, st) => Terminal(field.asText + " -> " + st, nodeType("SortKey")) }.toList,
                                    nodeType("SortOp"))
      case GeoNearOp(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)
                                => NonTerminal("",
                                    WorkflowOpRenderTree.render(src) ::
                                      Terminal(near.toString, nodeType("GeoNearOp") :+ "Near") ::
                                      Terminal(distanceField.toString, nodeType("GeoNearOp") :+ "DistanceField") ::
                                      Terminal(limit.toString, nodeType("GeoNearOp") :+ "Limit") ::
                                      Terminal(maxDistance.toString, nodeType("GeoNearOp") :+ "MaxDistance") ::
                                      Terminal(query.toString, nodeType("GeoNearOp") :+ "Query") ::
                                      Terminal(spherical.toString, nodeType("GeoNearOp") :+ "Spherical") ::
                                      Terminal(distanceMultiplier.toString, nodeType("GeoNearOp") :+ "DistanceMultiplier") ::
                                      Terminal(includeLocs.toString, nodeType("GeoNearOp") :+ "IncludeLocs") ::
                                      Terminal(uniqueDocs.toString, nodeType("GeoNearOp") :+ "UniqueDocs") ::
                                      Nil, 
                                    nodeType("GeoNearOp"))
      case MapOp(src, fn) => NonTerminal("", WorkflowOpRenderTree.render(src) :: Terminal(fn.toString, nodeType("MapReduce")) :: Nil, nodeType("MapOp"))
      case FlatMapOp(src, fn) => NonTerminal("", WorkflowOpRenderTree.render(src) :: Terminal(fn.toString, nodeType("MapReduce")) :: Nil, nodeType("FlatMapOp"))
      case ReduceOp(src, fn) => NonTerminal("", WorkflowOpRenderTree.render(src) :: Terminal(fn.toString, nodeType("MapReduce")) :: Nil, nodeType("ReduceOp"))
      case FoldLeftOp(lsrcs)    => NonTerminal("", lsrcs.toList.map(WorkflowOpRenderTree.render(_)), nodeType("FoldLeftOp"))
      case FoldLeftOp0(lsrcs)    => NonTerminal("", lsrcs.toList.map(WorkflowOpRenderTree.render(_)), nodeType("FoldLeftOp"))
      case JoinOp(ssrcs)        => NonTerminal("", ssrcs.toList.map(WorkflowOpRenderTree.render(_)), nodeType("JoinOp"))
    }
  }
}
