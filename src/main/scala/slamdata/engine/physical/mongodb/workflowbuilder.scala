package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import WorkflowTask._
import slamdata.engine.std.StdLib._

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
  case class UnsupportedDistinct(message: String) extends WorkflowBuilderError
  case class UnsupportedJoinCondition(func: Mapping) extends WorkflowBuilderError {
    def message = "Joining with " + func.name + " is not currently supported"
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

  def normalize: WorkflowOp = base match {
    case DocVar.ROOT(None) => graph.finish
    case base =>
      copy(graph = struct.shift(graph, base), base = DocVar.ROOT()).normalize
  }

  def build: Workflow = normalize.workflow

  def asLiteral = asExprOp.collect { case (x @ ExprOp.Literal(_)) => x }

  def expr1(f: DocVar => Error \/ ExprOp): Error \/ WorkflowBuilder =
    f(base).map { expr =>
      val that = WorkflowBuilder.fromExpr(graph, expr)
      copy(graph = that.graph, base = that.base)
    }

  def expr2(that: WorkflowBuilder)(f: (DocVar, DocVar) => Error \/ ExprOp):
      Error \/ WorkflowBuilder = {
    this.merge(that) { (lbase, rbase, list) =>
      f(lbase, rbase).flatMap {
        case DocVar.ROOT(None) => \/-((this applyLens _graph).set(list))
        case expr =>
          mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
            WorkflowBuilder(
              chain(list, projectOp(Reshape.Doc(ListMap(ExprName -> -\/ (expr))))),
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
          chain(list, projectOp(Reshape.Doc(ListMap(LeftName -> -\/ (lbase), RightName -> -\/ (rbase))))),
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

  def makeObject(name: String): Error \/ WorkflowBuilder =
    asExprOp.collect {
      case x : ExprOp.GroupOp => applyGroupBy(x, name)
    }.getOrElse {
      \/- (copy(
              graph = chain(graph, projectOp(Reshape.Doc(ListMap(BsonField.Name(name) -> -\/ (base))))),
              base = DocVar.ROOT(),
              struct = struct.makeObject(name)))
    }
  
  private def applyGroupBy(expr: ExprOp.GroupOp, name: String): Error \/ WorkflowBuilder =
    groupBy match {
      case Nil => -\/(WorkflowBuilderError.NotGrouped)

      case b :: bs =>
        val (construct, inner) = ExprOp.GroupOp.decon(expr)

        graph match {
          case me: WPipelineOp =>
            val rewritten =
              copy(
                graph = chain(me.src, projectOp(Reshape.Doc(ListMap(ExprName -> -\/(inner))))))

            rewritten.merge(b) { (grouped, by, list) =>
              \/- (WorkflowBuilder(
                chain(list, groupOp(Grouped(ListMap(BsonField.Name(name) -> construct(grouped))), -\/ (by))),
                DocVar.ROOT(),
                self.struct.makeObject(name),
                bs))
            }
          case _ => -\/(WorkflowBuilderError.InvalidGroup(graph))
        }
    }

  def makeArray: WorkflowBuilder = {
    copy(
      graph = chain(graph, projectOp(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/ (base))))),
      base = DocVar.ROOT(),
      struct = struct.makeArray(0))
  }

  def objectConcat(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    import SchemaChange._

    def mergeUnknownSchemas(entries: List[Js.Stmt]) =
      Js.Call(
        Js.Select(
          Js.AnonFunDecl(List("rez"), entries :+ Js.Return(Js.Ident("rez"))),
          "call"),
        List(Js.Ident("this"), Js.AnonObjDecl(Nil)))
    val jsBase = Js.Ident("this")

    this.merge(that) { (left, right, list) =>
      mergeGroups(this.groupBy, that.groupBy).flatMap { mergedGroups =>
        def builderWithUnknowns(src: WorkflowOp, fields: List[Js.Stmt]) =
          WorkflowBuilder(
            chain(src,
              mapOp(MapOp.mapMap(mergeUnknownSchemas(fields)))),
            ExprVar,
            Init,
            mergedGroups)

        (this.struct.simplify, that.struct.simplify) match {
          case (MakeObject(m1), MakeObject(m2)) =>
            def convert(root: DocVar) = (keys: Seq[String]) =>
              keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

            val leftTuples  = convert(left)(m1.keys.toSeq)
            val rightTuples = convert(right)(m2.keys.toSeq)
            \/-(WorkflowBuilder(
              chain(list,
                projectOp(Reshape.Doc(ListMap((leftTuples ++ rightTuples): _*)))),
              DocVar.ROOT(),
              MakeObject(m1 ++ m2),
              mergedGroups))
          case (Init, MakeObject(m)) =>
            \/-(builderWithUnknowns(
              list,
              List(JsGen.copyAllFields((fromDocVar(left)).toJs(jsBase))) ++
                m.toList.map { case (k, v) =>
                  JsGen.copyOneField(Js.Str(k), (fromDocVar(right \ BsonField.Name(k))).toJs(jsBase))
                }))
          case (MakeObject(m), Init) =>
            \/-(builderWithUnknowns(
              list,
              m.toList.map { case (k, v) =>
                JsGen.copyOneField(Js.Str(k), (fromDocVar(left \ BsonField.Name(k))).toJs(jsBase))
              } ++
                List(JsGen.copyAllFields((fromDocVar(right)).toJs(jsBase)))))
          case (Init, Init) =>
            \/-(builderWithUnknowns(
              list,
              List(
                JsGen.copyAllFields((fromDocVar(left)).toJs(jsBase)),
                JsGen.copyAllFields((fromDocVar(right)).toJs(jsBase)))))
          case (l @ FieldProject(s1, f1), r @ FieldProject(s2, f2)) =>
            def convert(root: DocVar) = (keys: Seq[String]) =>
              keys.map(BsonField.Name.apply).map(name => name -> -\/(root)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

            val leftTuples  = convert(left)(List(f1))
            val rightTuples = convert(right)(List(f2))
            \/-(builderWithUnknowns(
              chain(list,
                projectOp(Reshape.Doc(ListMap((leftTuples ++ rightTuples): _*)))),
              List(
                JsGen.copyAllFields(l.toJs(jsBase)),
                JsGen.copyAllFields(r.toJs(jsBase)))))
          case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
        }
      }
    }
  }

  def arrayConcat(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeArray(m1), s2 @ SchemaChange.MakeArray(m2)) =>
        def convert(root: DocVar) = (shift: Int, keys: Seq[Int]) => 
          (keys.map { index => 
            BsonField.Index(index + shift) -> -\/ (root \ BsonField.Index(index))
          }): Seq[(BsonField.Index, ExprOp \/ Reshape)]

        this.merge(that) { (left, right, list) =>
          val rightShift = m1.keys.max + 1
          val leftTuples  = convert(left)(0, m1.keys.toSeq)
          val rightTuples = convert(right)(rightShift, m2.keys.toSeq)

          mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
            WorkflowBuilder(
              chain(list, projectOp(Reshape.Arr(ListMap((leftTuples ++ rightTuples): _*)))),
              DocVar.ROOT(),
              SchemaChange.MakeArray(m1 ++ m2.map(t => (t._1 + rightShift) -> t._2)),
              mergedGroups)
          }
        }

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (WorkflowBuilderError.CannotObjectConcatExpr)
    }
  }

  def flattenObject: WorkflowBuilder = {
    val field = base.toJs
    copy(
      graph =
        chain(graph,
          flatMapOp(
            Js.AnonFunDecl(List("key"),
              List(
                Js.VarDef(List("rez" -> Js.AnonElem(Nil))),
                Js.ForIn(Js.Ident("attr"), field,
                  Js.Call(
                    Js.Select(Js.Ident("rez"), "push"),
                    List(
                      Js.AnonElem(List(
                        Js.Call(Js.Ident("ObjectId"), Nil),
                        Js.Access(field, Js.Ident("attr"))))))),
                Js.Return(Js.Ident("rez")))))),
      base = ExprVar)
  }

  def flattenArray: WorkflowBuilder =
    copy(graph = chain(graph, unwindOp(base)))

  def projectField(name: String): WorkflowBuilder =
    copy(
      graph = chain(graph, projectOp(Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Name(name)))))),
      base = ExprVar,
      struct = struct.projectField(name))
    
  def projectIndex(index: Int): WorkflowBuilder =
    copy(
      // TODO: Replace the map/reduce with this projection when
      //       https://jira.mongodb.org/browse/SERVER-4589 is fixed
      // graph = ProjectOp(graph, Reshape.Doc(ListMap(
      //   ExprName -> -\/ (base \ BsonField.Index(index))))),
      graph = chain(graph,
        mapOp(
          Js.AnonFunDecl(List("key"),
            List(
              Js.Return(Js.AnonElem(List(
                Js.Ident("key"),
                Js.Access(
                  Js.Select(Js.Ident("this"), ExprLabel),
                  Js.Num(index, false))))))))),
      base = ExprVar,
      struct = struct.projectIndex(index))

  def isGrouped = !groupBy.isEmpty

  def groupBy(that: WorkflowBuilder): WorkflowBuilder = {
    copy(groupBy = that :: groupBy)
  }

  def reduce(f: ExprOp => ExprOp.GroupOp): Error \/ WorkflowBuilder = {
    // TODO: Currently we cheat and defer grouping until we makeObject / 
    //       makeArray. Alas that's not guaranteed and we should find a 
    //       more reliable way.
    expr1(e => \/-(f(e)))
  }

  def sortBy(that: WorkflowBuilder, sortTypes: List[SortType]):
      Error \/ WorkflowBuilder = {
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
                mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
                  WorkflowBuilder(
                    chain(list, 
                      sortOp(NonEmptyList.nel(x, xs))),
                    sort,
                    self.struct,
                    mergedGroups)
                }
            }
          }

        case _ => -\/ (WorkflowBuilderError.InvalidSortBy)
      }
    }
  }

  def join(that: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: ExprOp, rightKey: Js.Expr):
      Error \/ WorkflowBuilder = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._
    import PipelineOp._

    val joinOnField: BsonField.Name = BsonField.Name("joinOn")
    val leftField: BsonField.Name = BsonField.Name("left")
    val rightField: BsonField.Name = BsonField.Name("right")
    val nonEmpty: Selector.SelectorExpr = Selector.NotExpr(Selector.Size(0))

    def padEmpty(side: BsonField): ExprOp =
      ExprOp.Cond(
        ExprOp.Eq(
          ExprOp.Size(ExprOp.DocField(side)),
          ExprOp.Literal(Bson.Int32(0))),
        ExprOp.Literal(Bson.Arr(List(Bson.Doc(ListMap())))),
        ExprOp.DocField(side))

    def buildProjection(l: ExprOp, r: ExprOp): WorkflowOp => SingleSourceOp = { src => 
      chain(src,
        projectOp(Reshape.Doc(ListMap(
          leftField -> -\/(l),
          rightField -> -\/(r)))),
        projectOp(Reshape.Doc(ListMap(
          ExprName -> -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, None))))))
    }

    def buildJoin(src: WorkflowOp, tpe: JoinType): SingleSourceOp =
      tpe match {
        case FullOuter => 
          chain(src,
            buildProjection(padEmpty(ExprName \ leftField), padEmpty(ExprName \ rightField)))
        case LeftOuter =>           
          chain(src,
            matchOp(Selector.Doc(ListMap(ExprName \ leftField -> nonEmpty))),
            buildProjection(
              ExprOp.DocField(ExprName \ leftField), padEmpty(ExprName \ rightField)))
        case RightOuter =>           
          chain(src,
            matchOp(Selector.Doc(ListMap(ExprName \ rightField -> nonEmpty))),
            buildProjection(
              padEmpty(ExprName \ leftField), ExprOp.DocField(ExprName \ rightField)))
        case Inner =>
          chain(
            src,
            matchOp(
              Selector.Doc(ListMap(
                ExprName \ leftField -> nonEmpty,
                ExprName \ rightField -> nonEmpty))))
      }

    def rightMap(keyExpr: Expr): AnonFunDecl =
      MapOp.mapKeyVal(
        keyExpr,
        AnonObjDecl(List(
          ("left", AnonElem(Nil)),
          ("right", AnonElem(List(Ident("this")))))))

    val rightReduce =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              ("left", AnonElem(Nil)),
              ("right", AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), "left"),
                  Call(Select(Select(Ident("result"), "left"), "concat"),
                    List(Select(Ident("value"), "left")))),
                BinOp("=",
                  Select(Ident("result"), "right"),
                  Call(Select(Select(Ident("result"), "right"), "concat"),
                    List(Select(Ident("value"), "right")))))))),
          Return(Ident("result"))))

    comp match {
      case relations.Eq =>
        \/-(WorkflowBuilder(
          chain(
            foldLeftOp(
              chain(
                this.graph,
                projectOp(Reshape.Doc(ListMap(
                    joinOnField -> -\/(leftKey),
                    leftField   -> -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, None))))),
                groupOp(
                  Grouped(ListMap(leftField -> ExprOp.AddToSet(ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(leftField))))),
                  -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(joinOnField)))),
                projectOp(Reshape.Doc(ListMap(
                    leftField -> -\/(DocVar.ROOT(leftField)),
                    rightField -> -\/(ExprOp.Literal(Bson.Arr(Nil)))))),
                projectOp(Reshape.Doc(ListMap(
                    ExprName -> -\/(ExprOp.DocVar(ExprOp.DocVar.ROOT, None)))))),
              chain(that.graph,
                mapOp(rightMap(rightKey)),
                reduceOp(rightReduce))),
            buildJoin(_, tpe),
            unwindOp(ExprOp.DocField(ExprName \ leftField)),
            unwindOp(ExprOp.DocField(ExprName \ rightField))),
          ExprVar,
          SchemaChange.Init))
      case _ => -\/(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(that: WorkflowBuilder) =
    this.join(that,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      ExprOp.Literal(Bson.Int64(1)), Js.Num(1, false))

  def >>> (op: WorkflowOp => WorkflowOp): WorkflowBuilder = {
    val (newGraph, newBase) = WorkflowOp.rewrite(op(graph), base)
    copy(graph = newGraph, base = newBase)
  }

  def squash: WorkflowBuilder = {
    if (graph.vertices.collect { case UnwindOp(_, _) => () }.isEmpty) this
    else
      copy(
        graph = struct.shift(graph, base) match {
          case op @ ProjectOp(_, _) =>
            op.set(BsonField.Name("_id"), -\/(ExprOp.Exclude))
          case op                   => op // FIXME: not excluding _id here
        },
        base = DocVar.ROOT())
  }

  def distinct: Error \/ WorkflowBuilder = {
    struct match {
      case SchemaChange.MakeObject(fields) => \/- (
          WorkflowBuilder(
            chain(graph,
              groupOp(
              Grouped(ListMap(
                        fields.keys.toList.map(name => 
                          BsonField.Name(name) -> ExprOp.First(base \ BsonField.Name(name))
                        ): _*)),
                \/- (Reshape.Doc(ListMap(
                        fields.keys.toList.map(name => 
                          BsonField.Name(name) -> -\/ (base \ BsonField.Name(name))
                        ): _*))))),
            base,
            struct,
            Nil))
            
      case _ => 
        base match {
          case ExprOp.DocVar(ExprOp.DocVar.ROOT, Some(name @ BsonField.Name(_))) => \/- (
            WorkflowBuilder(
              chain(graph,
                groupOp(
                  Grouped(ListMap(
                    name -> ExprOp.First(base)
                  )),
                  -\/ (base))),
              base,
              struct,
              Nil))
            
          case _ => -\/ (WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + struct + "; " + base + ")"))
        }
    }
  }

  def asExprOp = this match {
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

    val ((lbase, rbase), op) = this.graph merge that.graph
    f(lbase \\ this.base, rbase \\ that.base, op)
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
      val One = pure(Bson.Int64(1L))

      val maxLen = groupBys0.view.map(_.length).max

      val groupBys: List[List[WorkflowBuilder]] = groupBys0.toList.map(_.reverse.padTo(maxLen, One).reverse)

      type EitherError[X] = Error \/ X

      groupBys.transpose.map {
        case Nil => \/- (One)
        case x :: xs => xs.foldLeftM[EitherError, WorkflowBuilder](x) { (a, b) => 
          if (a == b) \/-(a) else a.makeArray arrayConcat b.makeArray
        }
      }.sequenceU
    }
}

object WorkflowBuilder {
  import WorkflowOp._
  import ExprOp.{DocVar}

  private val ExprLabel  = "value"
  val ExprName           = BsonField.Name(ExprLabel)
  val ExprVar            = ExprOp.DocVar.ROOT(ExprName)

  private val LeftLabel  = "lEft"
  private val LeftName   = BsonField.Name(LeftLabel)
  private val LeftVar    = DocVar.ROOT(LeftName)

  private val RightLabel = "rIght"
  private val RightName  = BsonField.Name(RightLabel)
  private val RightVar   = DocVar.ROOT(RightName)

  def read(coll: Collection) =
    WorkflowBuilder(readOp(coll), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) =
    WorkflowBuilder(pureOp(bson), DocVar.ROOT(), SchemaChange.Init)

  def fromExpr(src: WorkflowOp, expr: ExprOp): WorkflowBuilder =
    WorkflowBuilder(
      chain(src, projectOp(PipelineOp.Reshape.Doc(ListMap(ExprName -> -\/ (expr))))),
      ExprVar,
      SchemaChange.Init)

  val _graph  = mkLens[WorkflowBuilder, WorkflowOp]("graph")
  val _base   = mkLens[WorkflowBuilder, DocVar]("base")
  val _struct = mkLens[WorkflowBuilder, SchemaChange]("struct")
  
  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[WorkflowOp], RE: RenderTree[ExprOp]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def render(v: WorkflowBuilder) = NonTerminal("",
      RO.render(v.graph) ::
        RE.render(v.base) ::
        Terminal(v.struct.toString, "WorkflowBuilder" :: "SchemaChange" :: Nil) ::
        NonTerminal("", v.groupBy.map(WorkflowBuilderRenderTree(RO, RE).render(_)), "WorkflowBuilder" :: "GroupBy" :: Nil) ::
        Nil,
      "WorkflowBuilder" :: Nil)
  }
}
