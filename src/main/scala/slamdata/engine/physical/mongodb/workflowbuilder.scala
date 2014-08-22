package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fs.Path
import slamdata.engine.{Error}
import WorkflowTask._

import scalaz._
import Scalaz._
import monocle.Macro._
import monocle.syntax._

sealed trait WorkflowBuilderError extends Error
object WorkflowBuilderError {
  case object CouldNotPatchRoot extends WorkflowBuilderError {
    def message = "Could not patch ROOT"
  }
  case object CannotObjectConcatExpr extends WorkflowBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object CannotArrayConcatExpr extends WorkflowBuilderError {
    def message = "Cannot array concat an expression"
  }
  case object NotGrouped extends WorkflowBuilderError {
    def message = "The pipeline builder has not been grouped by another set, so a group op doesn't make sense"
  }
  case class InvalidGroup(op: WorkflowOp) extends WorkflowBuilderError {
    def message = "Can not group " + op
                                                            }
  case object InvalidSortBy extends WorkflowBuilderError {
    def message = "The sort by set has an invalid structure"
  }
  case object UnknownStructure extends WorkflowBuilderError {
    def message = "The structure is unknown due to a missing project or group operation"
  }
}

/**
 * A `WorkflowBuilder` consists of a graph of operations, a structure, and a
 * base mod for that structure.
 */
final case class WorkflowBuilder private (
  graph: WorkflowOp,
  base: ExprOp.DocVar,
  struct: SchemaChange,
  groupBy: List[WorkflowBuilder] = Nil) { self =>
  import WorkflowBuilder._
  import WorkflowOp._
  import PipelineOp._
  import ExprOp.{DocVar}

  def build: Error \/ Workflow = base match {
    case DocVar.ROOT(None) => \/-(graph.finish)
    case base =>
      struct match {
        case s @ SchemaChange.MakeObject(_) =>
          copy(graph = s.shift(graph, base), base = DocVar.ROOT()).build
        case s @ SchemaChange.MakeArray(_) =>
          copy(graph = s.shift(graph, base), base = DocVar.ROOT()).build
        case _ =>
          -\/(WorkflowBuilderError.UnknownStructure)
      }
  }

  def asLiteral = asExprOp.collect { case (x @ ExprOp.Literal(_)) => x }

  def expr1(f: DocVar => Error \/ ExprOp): Error \/ WorkflowBuilder = f(base).map { expr =>
    val that = WorkflowBuilder.fromExpr(graph, expr)
    copy(graph = that.graph, base = that.base)
  }

  def expr2(that: WorkflowBuilder)(f: (DocVar, DocVar) => Error \/ ExprOp): Error \/ WorkflowBuilder = {
    this.merge(that) { (lbase, rbase, list) =>
      f(lbase, rbase).flatMap {
        case DocVar.ROOT(None) => \/-((this applyLens _graph).set(list))
        case expr =>
          mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
            WorkflowBuilder(
              ProjectOp(list, Reshape.Doc(ListMap(ExprName -> -\/ (expr)))).coalesce,
              ExprVar,
              SchemaChange.Init,
              mergedGroups)
          }
      }
    }
  }

  def expr3(p2: WorkflowBuilder, p3: WorkflowBuilder)(f: (DocVar, DocVar, DocVar) => Error \/ ExprOp): Error \/ WorkflowBuilder = {
    val nest = (lbase: DocVar, rbase: DocVar, list: WorkflowOp) => {
      mergeGroups(this.groupBy, p2.groupBy, p3.groupBy).map { mergedGroups =>
        WorkflowBuilder(
          ProjectOp(list, Reshape.Doc(ListMap(LeftName -> -\/ (lbase), RightName -> -\/ (rbase)))).coalesce,
          DocVar.ROOT(),
          SchemaChange.Init,
          mergedGroups)
      }
    }

    for {
      p12    <- this.merge(p2)(nest)
      p123   <- p12.merge(p3)(nest)
      pfinal <- p123.expr1 { root =>
        f(root \ LeftName \ LeftName, root \ LeftName \ RightName, root \ RightName)
      }
    } yield pfinal
  }

  def map(f: (WorkflowOp, ExprOp.DocVar) => Error \/ WorkflowBuilder): Error \/ WorkflowBuilder =
    f(graph, base)

  def makeObject(name: String): Error \/ WorkflowBuilder = {
    asExprOp.collect {
      case x : ExprOp.GroupOp =>
        groupBy match {
          case Nil => -\/(WorkflowBuilderError.NotGrouped)

          case b :: bs =>
            val (construct, inner) = ExprOp.GroupOp.decon(x)

            graph match {
              case me: WPipelineOp =>
                val rewritten =
                  copy(
                    graph = ProjectOp(me.src, Reshape.Doc(ListMap(ExprName -> -\/(inner)))).coalesce)

                rewritten.merge(b) { (grouped, by, list) =>
                  \/- (WorkflowBuilder(
                    GroupOp(list, Grouped(ListMap(BsonField.Name(name) -> construct(grouped))), -\/ (by)).coalesce,
                    DocVar.ROOT(),
                    self.struct.makeObject(name),
                    bs))
                }
              case _ => -\/(WorkflowBuilderError.InvalidGroup(graph))
            }
        }
    }.getOrElse {
      \/- {
        copy(
          graph = ProjectOp(graph, Reshape.Doc(ListMap(BsonField.Name(name) -> -\/ (base)))).coalesce,
          base = DocVar.ROOT(),
          struct = struct.makeObject(name))
      }
    }
  }

  def makeArray: Error \/ WorkflowBuilder = {
    \/-(copy(
      graph = ProjectOp(graph, Reshape.Arr(ListMap(BsonField.Index(0) -> -\/ (base)))).coalesce,
      base = DocVar.ROOT(),
      struct = struct.makeArray(0)))
    
  }

  def objectConcat(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeObject(m1), s2 @ SchemaChange.MakeObject(m2)) =>
        def convert(root: DocVar) = (keys: Seq[String]) => 
          keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

        for {
          rez <-  this.merge(that) { (left, right, list) =>
                    val leftTuples  = convert(left)(m1.keys.toSeq)
                    val rightTuples = convert(right)(m2.keys.toSeq)

                    mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
                      WorkflowBuilder(
                        ProjectOp(list, Reshape.Doc(ListMap((leftTuples ++ rightTuples): _*))).coalesce,
                        DocVar.ROOT(),
                        SchemaChange.MakeObject(m1 ++ m2),
                        mergedGroups)
                    }
                  }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (WorkflowBuilderError.CannotObjectConcatExpr)
    }
  }

  def arrayConcat(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeArray(m1), s2 @ SchemaChange.MakeArray(m2)) =>
        def convert(root: DocVar) = (shift: Int, keys: Seq[Int]) => 
          (keys.map { index => 
            BsonField.Index(index + shift) -> -\/ (root \ BsonField.Index(index))
          }): Seq[(BsonField.Index, ExprOp \/ Reshape)]

        for {
          rez <-  this.merge(that) { (left, right, list) =>
            val rightShift = m1.keys.max + 1
            val leftTuples  = convert(left)(0, m1.keys.toSeq)
            val rightTuples = convert(right)(rightShift, m2.keys.toSeq)

            mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
              WorkflowBuilder(
                ProjectOp(list, Reshape.Arr(ListMap((leftTuples ++ rightTuples): _*))).coalesce,
                DocVar.ROOT(),
                SchemaChange.MakeArray(m1 ++ m2.map(t => (t._1 + rightShift) -> t._2)),
                mergedGroups)
            }
          }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (WorkflowBuilderError.CannotObjectConcatExpr)
    }
  }

  def flattenArray: Error \/ WorkflowBuilder =
    \/- (copy(graph = UnwindOp(graph, base).coalesce))

  def projectField(name: String): Error \/ WorkflowBuilder =
    \/- {
      copy(
        graph = ProjectOp(graph, Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Name(name))))).coalesce,
        base = ExprVar, 
        struct = struct.projectField(name))
    }

  def projectIndex(index: Int): Error \/ WorkflowBuilder =
    \/- {
      copy(
        graph = ProjectOp(graph, Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Index(index))))).coalesce,
        base = ExprVar,
        struct = struct.projectIndex(index))
    }

  def isGrouped = !groupBy.isEmpty

  def groupBy(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    \/- (copy(groupBy = that :: groupBy))
  }

  def reduce(f: ExprOp => ExprOp.GroupOp): Error \/ WorkflowBuilder = {
    // TODO: Currently we cheat and defer grouping until we makeObject / 
    //       makeArray. Alas that's not guaranteed and we should find a 
    //       more reliable way.
    expr1(e => \/-(f(e)))
  }

  def sortBy(that: WorkflowBuilder, sortTypes: List[SortType]): Error \/ WorkflowBuilder = {
    this.merge(that) { (sort, by, list) =>
      (that.struct.simplify, by) match {
        case (SchemaChange.MakeArray(els), DocVar(_, Some(by))) =>
          if (els.size != sortTypes.length) -\/ (WorkflowBuilderError.InvalidSortBy)
          else {
            val sortFields = (els.zip(sortTypes).foldLeft(List.empty[(BsonField, SortType)]) {
              case (acc, ((idx, s), sortType)) =>
                val index = BsonField.Index(idx)

                val key: BsonField = by \ index \ BsonField.Name("key")

                (key -> sortType) :: acc
            }).reverse

            sortFields match {
              case Nil => -\/ (WorkflowBuilderError.InvalidSortBy)

              case x :: xs => 
                mergeGroups(this.groupBy, that.groupBy).map { mergedGroups => // ???
                  WorkflowBuilder(
                    SortOp(list, NonEmptyList.nel(x, xs)),
                    sort,
                    self.struct,
                    mergedGroups)
                }
            }
          }

        case _ => 
          -\/ (WorkflowBuilderError.InvalidSortBy)
      }
    }
  }

  def &&& (op: WorkflowOp => WorkflowOp): Error \/ WorkflowBuilder = {
    this.merge(WorkflowBuilder(op(DummyOp), DocVar.ROOT(), SchemaChange.Init)) {
      (lbase, rbase, list) =>
      \/-(copy(
        graph = list,
        base   = DocVar.ROOT(),
        struct = SchemaChange.Init))
    }
  }

  def >>> (op: WorkflowOp => WorkflowOp): Error \/ WorkflowBuilder =
    \/-(copy(graph = op(graph)))

  def squash: Error \/ WorkflowBuilder = {
    if (graph.vertices.collect { case UnwindOp(_, _) => () }.isEmpty) \/-(this)
    else {
      val _Id = BsonField.Name("_id")

      struct match {
        case s @ SchemaChange.MakeObject(_) =>
          \/-(copy(
            graph = s.shift(graph, base).set(_Id, -\/ (ExprOp.Exclude)),
            base = DocVar.ROOT()))
        case s @ SchemaChange.MakeArray(_) =>
          \/-(copy(
            graph = s.shift(graph, base).set(_Id, -\/ (ExprOp.Exclude)),
            base = DocVar.ROOT()))
        case _ =>
          \/-(this)
          //-\/(WorkflowBuilderError.UnknownStructure)
      }
    }
  }

  def asExprOp = this applyLens _graph modify (_.coalesce) match {
    case WorkflowBuilder(ProjectOp(_, Reshape.Doc(fields)), `ExprVar`, _, _) =>
      fields.toList match {
        case (`ExprName`, -\/ (e)) :: Nil => Some(e)
        case _ => None
      }
    case WorkflowBuilder(PureOp(bson), _, _, _) =>
      Some(ExprOp.Literal(bson))
    case _ => None
  }

  // TODO: At least some of this should probably be deferred to
  //       WorkflowOp.coalesce.
  private def merge[A](that: WorkflowBuilder)(f: (DocVar, DocVar, WorkflowOp) => Error \/ A):
      Error \/ A = {
    type Out = Error \/ ((DocVar, DocVar), WorkflowOp)

    def rewrite[A <: WorkflowOp](op: A, base: DocVar): (A, DocVar) = {
      (op.rewriteRefs(PartialFunction(base \\ _))) -> (op match {
        case _ : GroupOp   => DocVar.ROOT()
        case _ : ProjectOp => DocVar.ROOT()
          
        case _ => base
      })
    }

    def step(left: (WorkflowOp, DocVar), right: (WorkflowOp, DocVar)): Out = {
      def delegate =
        step(right, left).map { case ((r, l), merged) => ((l, r), merged) }
      (left, right) match {
        case ((DummyOp, lbase), (right, rbase)) => \/-((lbase, rbase) -> right)
        case (_, (DummyOp, _)) => delegate
        case ((left @ PureOp(lval), lbase), (PureOp(rval), rbase))
            if lval == rval =>
          \/-((lbase, rbase) -> left)
        case ((left @ ReadOp(lcol), lbase), (ReadOp(rcol), rbase))
            if lcol == rcol =>
          \/-((lbase, rbase) -> left)
        case ((PureOp(lval), lbase), (right @ ReadOp(_), rbase)) =>
          val builder = WorkflowBuilder.fromExpr(right, ExprOp.Literal(lval))
          \/-((builder.base, rbase) -> builder.graph)
        case ((ReadOp(_), _), (PureOp(_), _)) => delegate
        case ((_: SourceOp, _), (_: SourceOp, _)) =>
          -\/(WorkflowBuilderError.CouldNotPatchRoot) // -\/("incompatible sources")
        case ((left : GeoNearOp, lbase), (r : WPipelineOp, rbase)) =>
          step((left, lbase), (r.src, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(r, rb)
              ((lb0, rb), right0.reparent(src))
          }
        case (_, (_ : GeoNearOp, _)) => delegate
        case ((left: WorkflowOp.ShapePreservingOp, lbase), (r: WPipelineOp, rbase)) =>
          step((left, lbase), (r.src, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(r, rb)
              ((lb0, rb), right0.reparent(src))
          }
        case ((_: WPipelineOp, _), (_: WorkflowOp.ShapePreservingOp, _)) => delegate
        case ((left @ GroupOp(lsrc, Grouped(_), b1), lbase), (right @ GroupOp(rsrc, Grouped(_), b2), rbase)) if (b1 == b2) =>
          step((lsrc, lbase), (rsrc, rbase)).map {
            case ((lb, rb), src) =>
              val (GroupOp(_, Grouped(g1_), b1), lb0) = rewrite(left, lb)
              val (GroupOp(_, Grouped(g2_), b2), rb0) = rewrite(right, rb)

              val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

              val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
              val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

              val g = g1 ++ g2
              val b = \/-(Reshape.Arr(ListMap(
                BsonField.Index(0) -> b1,
                BsonField.Index(1) -> b2)))

              ((lb0, rb0),
                ProjectOp.EmptyDoc(GroupOp(src, Grouped(g), b).coalesce).setAll(to.mapValues(f => -\/ (DocVar.ROOT(f)))).coalesce)
          }
        case ((left @ GroupOp(_, Grouped(_), _), lbase), (r: WPipelineOp, rbase)) =>
          step((left.src, lbase), (r, rbase)).map {
            case ((lb, rb), src) =>
              val (GroupOp(_, Grouped(g1_), b1), lb0) = rewrite(left, lb)
              val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))
              val uniqVar = DocVar.ROOT(uniqName)

              ((lb0, uniqVar) ->
                chain(src,
                  GroupOp(_, Grouped(g1_ + (uniqName -> ExprOp.Push(rb))), b1),
                  UnwindOp(_, uniqVar)).coalesce)
          }
        case ((_: WPipelineOp, _), (GroupOp(_, _, _), _)) => delegate
        case (
          (left @ ProjectOp(lsrc, _), lbase),
          (right @ ProjectOp(rsrc, _), rbase)) =>
          step((lsrc, lbase), (rsrc, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((LeftVar \\ lb0, RightVar \\ rb0) ->
                ProjectOp(src,
                  Reshape.Doc(ListMap(
                    LeftName -> \/-(left0.shape),
                    RightName -> \/-(right0.shape)))).coalesce)
          }
        case ((left @ ProjectOp(lsrc, _), lbase), (r: WPipelineOp, rbase)) =>
          step((lsrc, lbase), (r.src, rbase)).map {
            case ((lb, rb), op) =>
              val (left0, lb0) = rewrite(left, lb)
              ((LeftVar \\ lb0, RightVar \\ rb) ->
                ProjectOp(op,
                  Reshape.Doc(ListMap(
                    LeftName -> \/- (left0.shape),
                    RightName -> -\/ (DocVar.ROOT())))).coalesce)
          }
        case ((_: WPipelineOp, _), (ProjectOp(_, _), _)) => delegate
        case ((left @ RedactOp(lsrc, _), lbase), (right @ RedactOp(rsrc, _), rbase)) =>
          step((lsrc, lbase), (rsrc, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((lb0, rb0), RedactOp(RedactOp(src, left0.value).coalesce, right0.value).coalesce)
          }
        case ((left @ UnwindOp(lsrc, lfield), lbase), (right @ UnwindOp(rsrc, rfield), rbase)) if lfield == rfield =>
          step((lsrc, lbase), (rsrc, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((lb0, rb0), UnwindOp(src, left0.field))
          }
        case ((left @ UnwindOp(lsrc, _), lbase), (right @ UnwindOp(rsrc, _), rbase)) =>
          step((lsrc, lbase), (rsrc, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((lb0, rb0), UnwindOp(UnwindOp(src, left0.field).coalesce, right0.field).coalesce)
          }
        case ((left @ UnwindOp(lsrc, lfield), lbase), (right @ RedactOp(_, _), rbase)) =>
          step((lsrc, lbase), (right, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((lb0, rb0), left0.reparent(src))
          }
        case ((RedactOp(_, _), _), (UnwindOp(_, _), _)) => delegate
        case ((left: WorkflowOp, lbase), (right: WPipelineOp, rbase)) =>
          step((left, lbase), (right.src, rbase)).map {
            case ((lb, rb), src) =>
              val (left0, lb0) = rewrite(left, lb)
              val (right0, rb0) = rewrite(right, rb)
              ((lb0, rb0), right0.reparent(src))
          }
        case ((_: WPipelineOp, _), (_: WorkflowOp, _)) => delegate
        case _ =>
          -\/(WorkflowBuilderError.UnknownStructure) // -\/("we’re screwed")
      }
    }

    step((this.graph, DocVar.ROOT()), (that.graph, DocVar.ROOT())).flatMap {
      case ((lbase, rbase), op) => f(lbase \\ this.base, rbase \\ that.base, op)
    }
  }

  private def mergeGroups(groupBys0: List[WorkflowBuilder]*):
      Error \/ List[WorkflowBuilder] =
    if (groupBys0.isEmpty) \/-(Nil)
    else {
      /*
        p1    p2
        |     |
        a     d
        |
        b
        |
        c

           
        c     X
        |     |
        b     X
        |     |
        a     d


        a     d     -> merge to A
        |     |                 |
        b     X     -> merge to B
        |     |                 |
        c     X     -> merge to C
       */
      val One = WorkflowBuilder.fromExpr(DummyOp, ExprOp.Literal(Bson.Int64(1L)))

      val maxLen = groupBys0.view.map(_.length).max

      val groupBys: List[List[WorkflowBuilder]] = groupBys0.toList.map(_.reverse.padTo(maxLen, One).reverse)

      type EitherError[X] = Error \/ X

      groupBys.transpose.map {
        case Nil => \/- (One)
        case x :: xs => xs.foldLeftM[EitherError, WorkflowBuilder](x) { (a, b) => 
          if (a == b) \/- (a) else for {
            a <- a.makeArray
            b <- b.makeArray
            c <- a.arrayConcat(b)
          } yield c
        }
      }.sequenceU
    }
}

object WorkflowBuilder {
  import WorkflowOp._
  import ExprOp.{DocVar}

  private val ExprName  = BsonField.Name("expr")
  private val ExprVar   = ExprOp.DocVar.ROOT(ExprName)
  private val LeftName  = BsonField.Name("lEft")
  private val RightName = BsonField.Name("rIght")

  private val LeftVar   = DocVar.ROOT(LeftName)
  private val RightVar  = DocVar.ROOT(RightName)

  def read(path: Path) = WorkflowBuilder(ReadOp(path), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) =
    // NB: Pre-convert pure ops, until merging works better.
    WorkflowBuilder.fromExpr(DummyOp, ExprOp.Literal(bson))
    // WorkflowBuilder(PureOp(bson), DocVar.ROOT(), SchemaChange.Init)

  def join(
    left: WorkflowBuilder, right: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType,
    leftKey: ExprOp, rightKey: Js.Expr):
      WorkflowBuilder = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._
    import PipelineOp._

    val joinOnField: BsonField.Name = BsonField.Name("joinOn")
    val leftField: BsonField.Name = BsonField.Name("left")
    val rightField: BsonField.Name = BsonField.Name("right")
    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): ExprOp =
      ExprOp.Cond(ExprOp.Not(ExprOp.Eq(ExprOp.Size(ExprOp.DocField(side)),
        ExprOp.Literal(Bson.Int32(0)))),
        ExprOp.DocField(side),
        ExprOp.Literal(Bson.Arr(List(Bson.Null))))

    def buildProjection(src: WorkflowOp, l: ExprOp, r: ExprOp): WorkflowOp =
      ProjectOp(src, Reshape.Doc(ListMap(leftField -> -\/(l), rightField -> -\/(r))))

    def buildJoin(src: WorkflowOp, tpe: JoinType): WorkflowOp =
      tpe match {
        case FullOuter => 
          buildProjection(src, padEmpty(leftField), padEmpty(rightField))
        case LeftOuter =>           
          buildProjection(
            MatchOp(src, Selector.Doc(ListMap(leftField.asInstanceOf[BsonField] -> nonEmpty))),
            ExprOp.Literal(Bson.Int32(1)), padEmpty(rightField))
        case RightOuter =>           
          buildProjection(
            MatchOp(src, Selector.Doc(ListMap(rightField.asInstanceOf[BsonField] -> nonEmpty))),
            padEmpty(leftField), ExprOp.Literal(Bson.Int32(1)))
        case Inner =>
          MatchOp(
            src,
            Selector.Doc(ListMap(leftField.asInstanceOf[BsonField] -> nonEmpty,
              rightField.asInstanceOf[BsonField] -> nonEmpty)))
      }

    def buildRightMap(keyExpr: Expr): AnonFunDecl =
      AnonFunDecl(Nil,
        List(Call(Ident("emit"),
          List(keyExpr, AnonObjDecl(List(
            ("left", AnonElem(Nil)),
            ("right", AnonElem(List(Ident("this"))))))))))

    def buildRightReduce: AnonFunDecl =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              ("left", AnonElem(Nil)),
              ("right", AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              List(
                Call(Select(Select(Ident("result"), "left"), "concat"),
                  List(Select(Ident("value"), "left"))),
                Call(Select(Select(Ident("result"), "right"), "concat"),
                  List(Select(Ident("value"), "right"))))))),
          Return(Ident("result"))))

    WorkflowBuilder(
      chain(
        FoldLeftOp(NonEmptyList(
          chain(
            left.graph,
            ProjectOp(_,
              Reshape.Doc(ListMap(
                joinOnField -> -\/(leftKey),
                leftField   -> -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, None))))),
            WorkflowOp.GroupOp(_,
              Grouped(ListMap(leftField -> ExprOp.AddToSet(ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(leftField))))),
              -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(joinOnField))))),
          MapReduceOp(right.graph,
            MapReduce(
              buildRightMap(rightKey),
              buildRightReduce,
              Some(MapReduce.WithAction(MapReduce.Action.Reduce)))))),
        buildJoin(_, tpe),
        UnwindOp(_, ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(leftField))),
        UnwindOp(_, ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(rightField)))),
      DocVar.ROOT(),
      SchemaChange.Init)
  }

  def fromExpr(src: WorkflowOp, expr: ExprOp): WorkflowBuilder =
    WorkflowBuilder(
      ProjectOp(src, PipelineOp.Reshape.Doc(ListMap(ExprName -> -\/ (expr)))).coalesce,
      ExprVar,
      SchemaChange.Init)  

  val _graph  = mkLens[WorkflowBuilder, WorkflowOp]("graph")
  val _base   = mkLens[WorkflowBuilder, DocVar]("base")
  val _struct = mkLens[WorkflowBuilder, SchemaChange]("struct")
}
