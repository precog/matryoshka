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
 * MapOp, it might make sense to collapse the ProjectOp into the mapping
 * function, but we no longer have a handle on the JS to make that happen.
 */
sealed trait WorkflowOp {
  def srcs: List[WorkflowOp]
  // def coalesce: WorkflowOp
  def crush: WorkflowTask

  // TODO: Automatically call `coalesce` when an op is created, rather than here
  //       and recursively in every overriden coalesce.
  def finish: WorkflowOp = this.deleteUnusedFields(Set.empty)

  def workflow: Workflow = Workflow(this.finish.crush)

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
      case ProjectOp(src, shape)     => chain(src, projectOp(applyReshape(shape)))
      case GroupOp(src, grouped, by) =>
        chain(src,
          groupOp(applyGrouped(grouped), by.bimap(applyExprOp _, applyReshape _)))
      case MatchOp(src, s)           => chain(src, matchOp(applySelector(s)))
      case RedactOp(src, e)          => chain(src, redactOp(applyExprOp(e)))
      case v @ LimitOp(src, _)       => v
      case v @ SkipOp(src, _)        => v
      case v @ UnwindOp(src, f)      => chain(src, unwindOp(applyVar(f)))
      case v @ SortOp(src, l)        => chain(src, sortOp(applyNel(l)))
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
    case p: SingleSourceOp  => p.reparent(f(p.src))
    case FoldLeftOp(srcs)   => FoldLeftOp.make(srcs.map(f))
    case JoinOp(srcs)       => JoinOp.make(srcs.map(f))
    // case OutOp(src, dst)    => OutOp.make(f(src), dst)
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
  
  def merge(that: WorkflowOp): ((DocVar, DocVar), WorkflowOp) = {
    import WorkflowBuilder.{ExprVar, ExprName, LeftLabel, LeftVar, LeftName, RightLabel, RightVar, RightName}
    
    def delegate = {
      val ((r, l), merged) = that merge this
      ((l, r), merged)
    }
    
    if (this == that)
      ((DocVar.ROOT(), DocVar.ROOT()) -> that)
    else
      (this, that) match {
        case (PureOp(lbson), PureOp(rbson)) =>
          ((LeftVar, RightVar) ->
            pureOp(Bson.Doc(ListMap(
              LeftLabel -> lbson,
              RightLabel -> rbson))))
        case (PureOp(bson), r) =>
          ((LeftVar, RightVar) ->
            chain(
              r,
              projectOp(Reshape.Doc(ListMap(
                LeftName -> -\/(ExprOp.Literal(bson)),
                RightName -> -\/(DocVar.ROOT()))))))
        case (_, PureOp(_)) => delegate

        case (left : GeoNearOp, r : WPipelineOp) =>
          val ((lb, rb), src) = left merge r.src
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(r, rb)
          ((lb0, rb), right0.reparent(src))
        case (_, _ : GeoNearOp) => delegate

        case (left: WorkflowOp.ShapePreservingOp, r: WPipelineOp) =>
          val ((lb, rb), src) = left merge r.src
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(r, rb)
          ((lb0, rb), right0.reparent(src))
        case (_: WPipelineOp, _: WorkflowOp.ShapePreservingOp) => delegate

        case (left @ ProjectOp(lsrc, shape), r: SourceOp) =>
          ((LeftVar, RightVar) ->
            chain(lsrc,
              projectOp(Reshape.Doc(ListMap(
                LeftName -> \/- (shape),
                RightName -> -\/ (DocVar.ROOT()))))))
        case (_: SourceOp, ProjectOp(_, _)) => delegate

        case (left @ GroupOp(lsrc, Grouped(_), b1), right @ GroupOp(rsrc, Grouped(_), b2)) if (b1 == b2) =>
          val ((lb, rb), src) = lsrc merge rsrc
          val (GroupOp(_, Grouped(g1_), b1), lb0) = rewrite(left, lb)
          val (GroupOp(_, Grouped(g2_), b2), rb0) = rewrite(right, rb)

          val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

          val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
          val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

          val g = g1 ++ g2
          val b = \/-(Reshape.Arr(ListMap(
              BsonField.Index(0) -> b1,
              BsonField.Index(1) -> b2)))

          ((lb0, rb0) ->
            ProjectOp.EmptyDoc(chain(src, groupOp(Grouped(g), b))).setAll(to.mapValues(f => -\/ (DocVar.ROOT(f)))))

        case (left @ GroupOp(_, Grouped(_), _), r: WPipelineOp) =>
          val ((lb, rb), src) = left.src merge r
          val (GroupOp(_, Grouped(g1_), b1), lb0) = rewrite(left, lb)
          val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))
          val uniqVar = DocVar.ROOT(uniqName)

          ((lb0, uniqVar) ->
            chain(src,
              groupOp(Grouped(g1_ + (uniqName -> ExprOp.Push(rb))), b1),
              unwindOp(uniqVar)))
        case (_: WPipelineOp, GroupOp(_, _, _)) => delegate

        case (left @ ProjectOp(lsrc, _), right @ ProjectOp(rsrc, _)) =>
          val ((lb, rb), src) = lsrc merge rsrc
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((LeftVar \\ lb0, RightVar \\ rb0) ->
            chain(src,
              projectOp(Reshape.Doc(ListMap(
                LeftName -> \/-(left0.shape),
                RightName -> \/-(right0.shape))))))

        case (left @ ProjectOp(lsrc, _), r: WPipelineOp) =>
          val ((lb, rb), op) = lsrc merge r.src
          val (left0, lb0) = rewrite(left, lb)
          ((LeftVar \\ lb0, RightVar \\ rb) ->
            chain(op,
              projectOp(Reshape.Doc(ListMap(
                LeftName -> \/- (left0.shape),
                RightName -> -\/ (DocVar.ROOT()))))))
        case (_: WPipelineOp, ProjectOp(_, _)) => delegate

        case (left @ RedactOp(lsrc, _), right @ RedactOp(rsrc, _)) =>
          val ((lb, rb), src) = lsrc merge rsrc
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((lb0, rb0) -> chain(src,
            redactOp(left0.value),
            redactOp(right0.value)))

        case (left @ UnwindOp(lsrc, lfield), right @ UnwindOp(rsrc, rfield)) if lfield == rfield =>
          val ((lb, rb), src) = lsrc merge rsrc
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((lb0, rb0) -> chain(src, 
            unwindOp(left0.field)))

        case (left @ UnwindOp(lsrc, _), right @ UnwindOp(rsrc, _)) =>
          val ((lb, rb), src) = lsrc merge rsrc
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((lb0, rb0) -> chain(src,
            unwindOp(left0.field),
            unwindOp(right0.field)))

        case (left @ UnwindOp(lsrc, lfield), right @ RedactOp(_, _)) =>
          val ((lb, rb), src) = lsrc merge right
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((lb0, rb0) -> left0.reparent(src))
        case (RedactOp(_, _), UnwindOp(_, _)) => delegate

        case (l @ ReadOp(_), MapOp(rsrc, fn)) =>
          val ((lb, rb), src) = l merge rsrc
          ((ExprVar \\ LeftVar \\ lb, ExprVar \\ RightVar) ->
            // TODO: we’re using src in 2 places here. Need #347’s `ForkOp`.
            foldLeftOp(
              chain(src,
                projectOp(Reshape.Doc(ListMap(
                    LeftName -> -\/(DocVar.ROOT())))),
                projectOp(Reshape.Doc(ListMap(
                  ExprName -> -\/(DocVar.ROOT()))))),
              chain(src,
                projectOp(
                  Reshape.Doc(ListMap(ExprName -> -\/(rb \\ ExprVar)))),
                mapOp(fn),
                projectOp(Reshape.Doc(ListMap(
                  RightName -> -\/(DocVar.ROOT())))),
                reduceOp(JsGen.foldLeftReduce))))
        case (MapOp(_, _), ReadOp(_)) => delegate

        case (left @ MapOp(_, _), r @ ProjectOp(rsrc, shape)) =>
          val ((lb, rb), src) = left merge rsrc
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(r, rb)
          ((LeftVar \\ lb0, RightVar \\ rb) ->
            chain(src,
              projectOp(Reshape.Doc(ListMap(
                LeftName -> -\/(DocVar.ROOT()),
                RightName -> \/-(shape))))))
        case (ProjectOp(_, _), MapOp(_, _)) => delegate

        case (left: WorkflowOp, right: WPipelineOp) =>
          val ((lb, rb), src) = left merge right.src
          val (left0, lb0) = rewrite(left, lb)
          val (right0, rb0) = rewrite(right, rb)
          ((lb0, rb0) -> right0.reparent(src))
        case (_: WPipelineOp, _: WorkflowOp) => delegate

        case (l, r) =>
          ((ExprVar \\ LeftVar, ExprVar \\ RightVar) ->
            foldLeftOp(
              chain(l,
                projectOp(Reshape.Doc(ListMap(
                  LeftName -> -\/(DocVar.ROOT())))),
                projectOp(Reshape.Doc(ListMap(
                  ExprName -> -\/(DocVar.ROOT()))))),
              chain(r,
                projectOp(Reshape.Doc(ListMap(
                    RightName -> -\/(DocVar.ROOT())))),
                reduceOp(JsGen.foldLeftReduce))))
      }
  }
}

object WorkflowOp {
  // probable conversions
  // to MapOp:          ProjectOp
  // to FlatMapOp:      MatchOp, LimitOp (using scope), SkipOp (using scope), UnwindOp, GeoNearOp
  // to MapOp/ReduceOp: GroupOp
  // ???:               RedactOp
  // none:              SortOp

  private val ExprLabel  = "value"
  private val ExprName   = BsonField.Name(ExprLabel)
  private val ExprVar    = ExprOp.DocVar.ROOT(ExprName)

  private val LeftLabel  = "lEft"
  private val LeftName   = BsonField.Name(LeftLabel)
  private val LeftVar    = ExprOp.DocVar.ROOT(LeftName)

  private val RightLabel = "rIght"
  private val RightName  = BsonField.Name(RightLabel)
  private val RightVar   = ExprOp.DocVar.ROOT(RightName)

  def rewrite[A <: WorkflowOp](op: A, base: ExprOp.DocVar): (A, ExprOp.DocVar) =
    (op.rewriteRefs(PartialFunction(base \\ _)) -> (op match {
      case GroupOp(_, _, _) => ExprOp.DocVar.ROOT()
      case ProjectOp(_, _)  => ExprOp.DocVar.ROOT()
      case _                => base
    }))

  /**
   * Operations without an input.
   */
  sealed trait SourceOp extends WorkflowOp {
    def srcs = Nil // Set.empty
  }

  /** Operations with a single source op. */
  sealed trait SingleSourceOp extends WorkflowOp {
    def src: WorkflowOp
    def reparent(newSrc: WorkflowOp): SingleSourceOp
  
    def srcs = List(src) // Set(src)
  }

  /**
   * This should be renamed once the other PipelineOp goes away, but it is the
   * subset of operations that can ever be pipelined.
   */
  sealed trait WPipelineOp extends SingleSourceOp {
    def pipeline: Option[(WorkflowTask, List[PipelineOp])]
  }
  sealed trait ShapePreservingOp extends WPipelineOp

  /**
   * Flattens the sequence of operations like so:
   * 
   *   chain(
   *     readOp(Path.fileAbs("foo")),
   *     matchOp(Selector.Where(Js.Bool(true))),
   *     limitOp(7))
   * ==
   *   val read = readOp(Path.fileAbs("foo"))
   *   val match = matchOp(Selector.Where(Js.Bool(true))(read)
   *   limitOp(7)(match)
   */
  def chain[A <: SingleSourceOp](src: WorkflowOp, op1: WorkflowOp => A, ops: (WorkflowOp => A)*): A =
    ops.foldLeft(op1(src))((s, o) => o(s))

  case class PureOp (value: Bson) extends SourceOp {
    def crush = PureTask(value)
  }
  val pureOp = PureOp.apply _

  case class ReadOp (coll: Collection) extends SourceOp {
    def crush = WorkflowTask.ReadTask(coll)
  }
  val readOp = ReadOp.apply _

  case class MatchOp private (src: WorkflowOp, selector: Selector) extends ShapePreservingOp {
    private def coalesce: ShapePreservingOp = src match {
      case SortOp(src0, value) => chain(src0, matchOp(selector), sortOp(value))
      case MatchOp(src0, sel0) => chain(src0, matchOp(Semigroup[Selector].append(sel0, selector)))
      case _ => this
    }
    def crush = {
      // TODO: If we ever allow explicit request of cursors (instead of
      //       collections), we could generate a FindQuery here.
      lazy val nonPipeline =
        MapReduceTask(
          src.crush,
          MapReduce(
            MapOp.mapFn(MapOp.mapNOP),
            ReduceOp.reduceNOP,
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
  object MatchOp {
    def make(selector: Selector)(src: WorkflowOp): ShapePreservingOp = MatchOp(src, selector).coalesce
  }
  val matchOp = MatchOp.make _

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

  case class ProjectOp private (src: WorkflowOp, shape: PipelineOp.Reshape)
      extends WPipelineOp {

    import PipelineOp._

    private def pipeop = PipelineOp.Project(shape)
    private def coalesce = src match {
      case ProjectOp(_, _) =>
        val (rs, src) = this.collectShapes
        inlineProject(rs.head, rs.tail).map(projectOp(_)(src)).getOrElse(this)
      case _ => this
    }
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp): ProjectOp = copy(src = newSrc)

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

    def make(shape: Reshape)(src: WorkflowOp): ProjectOp = ProjectOp(src, shape).coalesce

    val EmptyDoc = (src: WorkflowOp) => ProjectOp(src, Reshape.EmptyDoc)
    val EmptyArr = (src: WorkflowOp) => ProjectOp(src, Reshape.EmptyArr)   
  }
  val projectOp = ProjectOp.make _

  case class RedactOp private (src: WorkflowOp, value: ExprOp) extends WPipelineOp {
    private def pipeop = PipelineOp.Redact(value)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object RedactOp {
    def make(value: ExprOp)(src: WorkflowOp): RedactOp = RedactOp(src, value)
  }
  val redactOp = RedactOp.make _

  case class LimitOp private (src: WorkflowOp, count: Long) extends ShapePreservingOp {
    import MapReduce._

    private def pipeop = PipelineOp.Limit(count)
    private def coalesce = src match {
      case LimitOp(src0, count0) =>
        chain(src0,
          limitOp(Math.min(count0, count)))
      case SkipOp(src0, count0) =>
        chain(src0,
          limitOp(count0 + count),
          skipOp(count0))
      case _ => this
    }
    // TODO: If the preceding is a MatchOp, and it or its source isn’t
    //       pipelineable, then return a FindQuery combining the match and this
    //       limit
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object LimitOp {
    def make(count: Long)(src: WorkflowOp): ShapePreservingOp = LimitOp(src, count).coalesce
  }
  val limitOp = LimitOp.make _

  case class SkipOp private (src: WorkflowOp, count: Long) extends ShapePreservingOp {
    private def pipeop = PipelineOp.Skip(count)
    private def coalesce: SkipOp = src match {
      case SkipOp(src0, count0) => SkipOp(src0, count0 + count).coalesce
      case _                    => this
    }
    // TODO: If the preceding is a MatchOp (or a limit preceded by a MatchOp),
    //       and it or its source isn’t pipelineable, then return a FindQuery
    //       combining the match and this skip
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object SkipOp {
    def make(count: Long)(src: WorkflowOp): ShapePreservingOp = SkipOp(src, count).coalesce
  }
  val skipOp = SkipOp.make _

  case class UnwindOp private (src: WorkflowOp, field: ExprOp.DocVar)
      extends WPipelineOp {
    private def pipeop = PipelineOp.Unwind(field)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object UnwindOp {
    def make(field: ExprOp.DocVar)(src: WorkflowOp): UnwindOp = UnwindOp(src, field)
  }
  val unwindOp = UnwindOp.make _
  
  case class GroupOp private (
    src: WorkflowOp,
    grouped: PipelineOp.Grouped,
    by: ExprOp \/ PipelineOp.Reshape)
      extends WPipelineOp {

    import PipelineOp._

    // TODO: Not all GroupOps can be pipelined. Need to determine when we may
    //       need the group command or a map/reduce.
    private def pipeop = PipelineOp.Group(grouped, by)
    private def coalesce = inlineGroupProjects(this).map((GroupOp.apply _).tupled).getOrElse(this)

    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)

    def toProject: ProjectOp = grouped.value.foldLeft(projectOp(PipelineOp.Reshape.EmptyArr)(src)) {
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
  object GroupOp {
    def make(grouped: PipelineOp.Grouped, by: ExprOp \/ PipelineOp.Reshape)(src: WorkflowOp): GroupOp =
      GroupOp(src, grouped, by).coalesce
  }
  val groupOp = GroupOp.make _

  case class SortOp private (src: WorkflowOp, value: NonEmptyList[(BsonField, SortType)])
      extends ShapePreservingOp {
    private def pipeop = PipelineOp.Sort(value)
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object SortOp {
    def make(value: NonEmptyList[(BsonField, SortType)])(src: WorkflowOp): SortOp = SortOp(src, value)
  }
  val sortOp = SortOp.make _

  /**
   * TODO: If an OutOp has anything after it, we need to either do
   *   SeqOp(OutOp(src, dst), after(ReadOp(dst), ...))
   * or
   *   ForkOp(src, List(OutOp(_, dst), after(_, ...)))
   * The latter seems preferable, but currently the forking semantics are not
   * clear.
   */
  // case class OutOp private (src: WorkflowOp, collection: Collection) extends ShapePreservingOp {
  //   def coalesce = src.coalesce match {
  //     case read @ ReadOp(_) => read
  //     case _                => this
  //   }
  //   def pipeline = Some(alwaysPipePipe(src, PipelineOp.Out(field)))
  // }

  case class GeoNearOp private (src: WorkflowOp,
                                 near: (Double, Double), distanceField: BsonField,
                                 limit: Option[Int], maxDistance: Option[Double],
                                 query: Option[FindQuery], spherical: Option[Boolean],
                                 distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
                                 uniqueDocs: Option[Boolean])
      extends WPipelineOp {
    private def pipeop = PipelineOp.GeoNear(near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)
    private def coalesce: WorkflowOp = src match {
      case _: GeoNearOp   => this  // TODO: merge the params?
      case p: WPipelineOp => p.reparent(copy(src = p.src))
      case _              => this
    }
    def crush = alwaysCrushPipe(src, pipeop)
    def pipeline = Some(alwaysPipePipe(src, pipeop))
    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object GeoNearOp {
    def make(near: (Double, Double), distanceField: BsonField,
             limit: Option[Int], maxDistance: Option[Double],
             query: Option[FindQuery], spherical: Option[Boolean],
             distanceMultiplier: Option[Double], includeLocs: Option[BsonField],
             uniqueDocs: Option[Boolean])(src: WorkflowOp): WorkflowOp = 
     GeoNearOp(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs).coalesce
  }
  val geoNearOp = GeoNearOp.make _

  /**
    Takes a function of one parameter. The document itself is in the `this`
    parameter and the passed-in parameter is the current key (which defaults to
    `this._id`, but may have been overridden by previous Map/FlatMapOps). The
    function must return a 2-element array containing the new key and new value.
    */
  case class MapOp private (src: WorkflowOp, fn: Js.AnonFunDecl) extends SingleSourceOp {
    import MapOp._
    import Js._

    private def coalesce: SingleSourceOp = src match {
      case MapOp(src0, fn0) =>
        chain(src0,
          mapOp(
            AnonFunDecl(List("key"),
              List(
                VarDef(List(
                  "rez" -> Call(Select(fn0, "call"),
                    List(Ident("this"), Ident("key"))))),
                Return(Call(Select(fn, "call"),
                  List(
                    Access(Ident("rez"), Num(1, false)),
                    Access(Ident("rez"), Num(0, false)))))))))
      case FlatMapOp(src0, fn0) =>
        chain(src0,
          flatMapOp(
            AnonFunDecl(List("key"),
              List(Call(
                Select(Call(Select(fn0, "call"),
                  List(Ident("this"), Ident("key"))), "map"),
                List(AnonFunDecl(List("args"),
                  List(Return(Call(Select(fn, "call"), List(
                    Access(Ident("args"), Num(1, false)),
                    Access(Ident("args"), Num(0, false)))))))))))))
      case csrc => MapOp(csrc, fn)
    }

    private def newMR(src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      MapReduceTask(src, MapReduce(mapFn(this.fn), ReduceOp.reduceNOP, selection = sel, inputSort = sort, limit = count))

    def crush = src.crush match {
      case MapReduceTask(src0, mr @ MapReduce(_, _, _, _, _, _, None, _, _, _)) =>
        MapReduceTask(src0, mr applyLens MapReduce._finalizer set Some(finalizerFn(this.fn)))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
        newMR(src0, Some(sel), None, None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
        newMR(src0, None, Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
        newMR(src0, None, None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
        newMR(src0, Some(sel), Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, None, Some(sort), Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), Some(sort), Some(count))
      case srcTask =>
        newMR(srcTask, None, None, None)
    }

    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object MapOp {
    import Js._

    def make(fn: Js.AnonFunDecl)(src: WorkflowOp): SingleSourceOp = MapOp(src, fn).coalesce

    def mapKeyVal(key: Js.Expr, value: Js.Expr) =
      AnonFunDecl(List("key"),
        List(Return(AnonElem(List(key, value)))))
    def mapMap(transform: Js.Expr) =
      mapKeyVal(Ident("key"), transform)
    val mapNOP = mapMap(Ident("this"))

    def finalizerFn(fn: Js.Expr) =
      AnonFunDecl(List("key", "value"),
        List(Return(Access(
          Call(Select(fn, "call"), List(Ident("value"), Ident("key"))),
          Num(1, false)))))

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(Call(Select(Ident("emit"), "apply"),
          List(
            Null,
            Call(Select(fn, "call"),
              List(Ident("this"), Select(Ident("this"), "_id")))))))
  }
  val mapOp = MapOp.make _

  /**
    Takes a function of one parameter. The document itself is in the `this`
    parameter and the passed-in parameter is the current key (which defaults to
    `this._id`, but may have been overridden by previous Map/FlatMapOps). The
    function must return an array of 2-element arrays, each containing a new
    key and a new value.
    */
  case class FlatMapOp private (src: WorkflowOp, fn: Js.AnonFunDecl) extends SingleSourceOp {
    import FlatMapOp._
    import Js._

    private def coalesce: FlatMapOp = src match {
      case MapOp(src0, fn0) =>
        FlatMapOp(src0,
          AnonFunDecl(List("key"),
            List(
              VarDef(List(
                "rez" -> Call(Select(fn0, "call"),
                  List(Ident("this"), Ident("key"))))),
              Return(Call(Select(fn, "call"),
                List(
                  Access(Ident("rez"), Num(1, false)),
                  Access(Ident("rez"), Num(0, false))))))))
      case FlatMapOp(src0, fn0) =>
        FlatMapOp(src0,
          AnonFunDecl(List("key"),
            List(Return(Call(
              Select(Select(AnonElem(Nil), "concat"), "apply"),
              List(
                Null,
                Call(
                  Select(Call(Select(fn0, "call"),
                    List(Ident("this"), Ident("key"))), "map"),
                  List(AnonFunDecl(List("args"),
                    List(Return(Call(Select(fn, "call"), List(
                      Access(Ident("args"), Num(1, false)),
                      Access(Ident("args"), Num(0, false)))))))))))))))
      case csrc => FlatMapOp(csrc, fn)
    }

    private def newMR(src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      MapReduceTask(src, MapReduce(mapFn(this.fn), ReduceOp.reduceNOP, selection = sel, inputSort = sort, limit = count))

    def crush = src.crush match {
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
        newMR(src0, Some(sel), None, None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
        newMR(src0, None, Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
        newMR(src0, None, None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
        newMR(src0, Some(sel), Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, None, Some(sort), Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), Some(sort), Some(count))
      case srcTask =>
        newMR(srcTask, None, None, None)
    }

    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object FlatMapOp {
    import Js._

    def make(fn: Js.AnonFunDecl)(src: WorkflowOp): FlatMapOp = FlatMapOp(src, fn).coalesce

    def mapFn(fn: Js.Expr) =
      AnonFunDecl(Nil,
        List(
          Call(
            Select(Call(Select(fn, "call"),
              List(
                Ident("this"),
                Select(Ident("this"), "_id"))),
              "map"),
            List(AnonFunDecl(List("__rez"), List(Call(Select(Ident("emit"), "apply"), List(Null, Ident("__rez")))))))))
  }
  val flatMapOp = FlatMapOp.make _

  /**
    Takes a function of two parameters – a key and an array of values. The
    function must return a single value.
    */
  case class ReduceOp private (src: WorkflowOp, fn: Js.AnonFunDecl) extends SingleSourceOp {
    import ReduceOp._

    private def newMR(src: WorkflowTask, sel: Option[Selector], sort: Option[NonEmptyList[(BsonField, SortType)]], count: Option[Long]) =
      MapReduceTask(src, MapReduce(MapOp.mapFn(MapOp.mapNOP), this.fn, selection = sel, inputSort = sort, limit = count))

    def crush = src.crush match {
      case MapReduceTask(src0, mr @ MapReduce(_, reduceNOP, _, _, _, _, None, _, _, _)) =>
        MapReduceTask(src0, mr applyLens MapReduce._reduce set this.fn)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel)))) =>
        newMR(src0, Some(sel), None, None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort)))) =>
        newMR(src0, None, Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Limit(count)))) =>
        newMR(src0, None, None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort)))) =>
        newMR(src0, Some(sel), Some(sort), None)
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), None, Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, None, Some(sort), Some(count))
      case PipelineTask(src0, Pipeline(List(PipelineOp.Match(sel), PipelineOp.Sort(sort), PipelineOp.Limit(count)))) =>
        newMR(src0, Some(sel), Some(sort), Some(count))
      case srcTask =>
        newMR(srcTask, None, None, None)
    }

    def reparent(newSrc: WorkflowOp) = copy(src = newSrc)
  }
  object ReduceOp {
    def make(fn: Js.AnonFunDecl)(src: WorkflowOp): ReduceOp = ReduceOp(src, fn)
    
    val reduceNOP =
      Js.AnonFunDecl(List("key", "values"),
        List(Js.Return(Js.Access(Js.Ident("values"), Js.Num(0, false)))))
  }
  val reduceOp = ReduceOp.make _

  /**
    Performs a sequence of operations, sequentially, merging their results.
    */
  case class FoldLeftOp private (lsrcs: NonEmptyList[WorkflowOp]) extends WorkflowOp {
    def srcs = lsrcs.toList
    private def coalesce = lsrcs match {
      case NEL(FoldLeftOp(csrcs0), tail) => FoldLeftOp.make(csrcs0 :::> tail)
      case _                             => this
    }
    def crush =
      (lsrcs.head.crush, lsrcs.tail) match {
        case (first, second :: rest) => 
          FoldLeftTask(first, NonEmptyList.nel(second, rest).map(_.crush match {
            case MapReduceTask(src, mr) =>
              // FIXME: FoldLeftOp currently always reduces, but in future we’ll want
              //        to have more control.
              MapReduceTask(src,
                mr applyLens MapReduce._out set Some(MapReduce.WithAction(MapReduce.Action.Reduce)))
            case src => sys.error("not a mapReduce: " + src)  // FIXME: need to rewrite as a mapReduce
          }))
          
        case (head, Nil) => head
      }
  }
  object FoldLeftOp {
    def make(lsrcs: NonEmptyList[WorkflowOp]): FoldLeftOp = FoldLeftOp(lsrcs).coalesce
  }
  def foldLeftOp(head: WorkflowOp, tail: WorkflowOp*) = FoldLeftOp.make(NonEmptyList.nel(head, tail.toList))

  case class JoinOp private (ssrcs: Set[WorkflowOp]) extends WorkflowOp {
    def srcs = ssrcs.toList
    def crush = JoinTask(ssrcs.map(_.crush))
  }
  object JoinOp {
    def make(ssrcs: Set[WorkflowOp]): JoinOp = JoinOp(ssrcs)
  }
  val joinOp = JoinOp.make _
  
  implicit def WorkflowOpRenderTree(implicit RS: RenderTree[Selector], RE: RenderTree[ExprOp], RG: RenderTree[PipelineOp.Grouped], RJ: RenderTree[Js]): RenderTree[WorkflowOp] = new RenderTree[WorkflowOp] {
    def nodeType(subType: String) = "WorkflowOp" :: subType :: Nil

    def chain(op: SingleSourceOp): List[WorkflowOp] = {
      def loop(op: SingleSourceOp, acc: List[WorkflowOp]): List[WorkflowOp] = {
        val foo = op :: acc
        op.src match {
          case src: SingleSourceOp => loop(src, foo)
          case src                 => src :: foo
        }
      }
      loop(op, Nil)
    }

    def renderFlat(op: WorkflowOp) = op match {
      case PureOp(value)         => Terminal(value.toString, nodeType("PureOp"))
      case ReadOp(coll)          => Terminal(coll.name, nodeType("ReadOp"))
      case MatchOp(src, sel)     => NonTerminal("",
                                    RS.render(sel) ::
                                      Nil,
                                    nodeType("MatchOp"))
      case ProjectOp(src, shape) => NonTerminal("",
                                      PipelineOp.renderReshape("Shape", "", shape) ::
                                        Nil,
                                      nodeType("ProjectOp"))
      case RedactOp(src, value) => NonTerminal("", 
                                      RE.render(value) ::
                                        Nil,
                                    nodeType("RedactOp"))
      case LimitOp(src, count)  => Terminal(count.toString, nodeType("LimitOp"))
      case SkipOp(src, count)   => Terminal(count.toString, nodeType("SkipOp"))
      case UnwindOp(src, field) => Terminal(field.toString, nodeType("UnwindOp"))
      case GroupOp(src, grouped, -\/ (expr))
                                => NonTerminal("",
                                    WorkflowOpRenderTree.render(src) ::
                                      RG.render(grouped) ::
                                      Terminal(expr.toString, nodeType("By")) ::
                                      Nil,
                                    nodeType("GroupOp"))
      case GroupOp(src, grouped, \/- (by))
                                => NonTerminal("",
                                    RG.render(grouped) ::
                                      PipelineOp.renderReshape("By", "", by) ::
                                      Nil,
                                    nodeType("GroupOp"))
      case SortOp(src, value)   => NonTerminal("",
                                    value.map { case (field, st) => Terminal(field.asText + " -> " + st, nodeType("SortKey")) }.toList,
                                    nodeType("SortOp"))
      case GeoNearOp(src, near, distanceField, limit, maxDistance, query, spherical, distanceMultiplier, includeLocs, uniqueDocs)
                                => NonTerminal("",
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

      case MapOp(src, fn)       => NonTerminal("", RJ.render(fn) :: Nil, nodeType("MapOp"))
      case FlatMapOp(src, fn)   => NonTerminal("", RJ.render(fn) :: Nil, nodeType("FlatMapOp"))
      case ReduceOp(src, fn)    => NonTerminal("", RJ.render(fn) :: Nil, nodeType("ReduceOp"))

      case op                   => render(op)
    }

    def render(v: WorkflowOp) = v match {
      case op: SourceOp         => renderFlat(op)

      case op: SingleSourceOp   => NonTerminal("", chain(op).map(renderFlat(_)), nodeType("Chain"))

      case FoldLeftOp(lsrcs)    => NonTerminal("", lsrcs.toList.map(WorkflowOpRenderTree.render(_)), nodeType("FoldLeftOp"))
      case JoinOp(ssrcs)        => NonTerminal("", ssrcs.toList.map(WorkflowOpRenderTree.render(_)), nodeType("JoinOp"))
    }
  }
}

object JsGen {
  def copyOneField(key: Js.Expr, expr: Js.Expr) =
    Js.BinOp("=", Js.Access(Js.Ident("rez"), key), expr)

  def copyAllFields(expr: Js.Expr) =
      Js.ForIn(Js.Ident("attr"), expr,
        Js.If(
          Js.Call(Js.Select(expr, "hasOwnProperty"), List(Js.Ident("attr"))),
          copyOneField(Js.Ident("attr"), Js.Access(expr, Js.Ident("attr"))),
          None))

  val foldLeftReduce = {
    import Js._
    AnonFunDecl(List("key", "values"),
      List(
        VarDef(List("rez" -> AnonObjDecl(Nil))),
        Call(Select(Ident("values"), "forEach"),
          List(AnonFunDecl(List("value"),
            List(copyAllFields(Ident("value")))))),
        Return(Ident("rez"))))
  }
}