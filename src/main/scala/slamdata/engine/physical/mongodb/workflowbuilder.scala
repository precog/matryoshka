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
  struct: SchemaChange) { self =>
  import WorkflowBuilder._
  import WorkflowOp._
  import PipelineOp._
  import ExprOp.{DocVar}
  import IdHandling._

  def build: WorkflowOp = base match {
    case DocVar.ROOT(None) => flattenGrouped(graph).finish
    case base =>
      val g1 = struct.shift(graph, base).map(t => chain(graph, WorkflowOp.projectOp(t._1, t._2))).getOrElse(graph)
      copy(graph = g1, base = DocVar.ROOT()).build
  }

  private def flattenGrouped(op: WorkflowOp): WorkflowOp = {
    val pushed = op match {
      case GroupOp(_, Grouped(grouped), _) => 
        grouped.collect { 
          case (name, ExprOp.Push(_)) => name
        }
      
      case _ => Nil
    }
    pushed.foldLeft(op)((op, name) => chain(op, unwindOp(ExprOp.DocField(name))))
  }

  private def projectOp(shape: Reshape): WorkflowOp => WPipelineOp =
    WorkflowOp.projectOp(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(Function.const(IncludeId)))

  def asLiteral = asExprOp.collect { case (x @ ExprOp.Literal(_)) => x }

  def expr1(f: DocVar => Error \/ ExprOp): Error \/ WorkflowBuilder =
    f(base).map { expr =>
      val that = WorkflowBuilder.fromExpr(graph, expr)
      copy(graph = that.graph, base = that.base)
    }

  def expr2(that: WorkflowBuilder)(f: (DocVar, DocVar) => Error \/ ExprOp):
      Error \/ WorkflowBuilder = {
    this.merge(that) { (lbase, rbase, list) =>
      f(lbase, rbase).map {
        case DocVar.ROOT(None) => (this applyLens _graph).set(list)
        case expr =>
            WorkflowBuilder(
              chain(list, projectOp(Reshape.Doc(ListMap(ExprName -> -\/ (expr))))),
              ExprVar,
              SchemaChange.Init)
      }
    }
  }

  def expr3(p2: WorkflowBuilder, p3: WorkflowBuilder)(f: (DocVar, DocVar, DocVar) => Error \/ ExprOp): Error \/ WorkflowBuilder = {
    val nest = (lbase: DocVar, rbase: DocVar, list: WorkflowOp) => {
        \/- (WorkflowBuilder(
          chain(list, projectOp(Reshape.Doc(ListMap(LeftName -> -\/ (lbase), RightName -> -\/ (rbase))))),
          DocVar.ROOT(),
          SchemaChange.Init))
    }

    for {
      p12    <- this.merge(p2)(nest)
      p123   <- p12.merge(p3)(nest)
      pfinal <- p123.expr1 { root =>
        f(root \ LeftName \ LeftName, root \ LeftName \ RightName, root \ RightName)
      }
    } yield pfinal
  }

  def makeObject(name: String): WorkflowBuilder =
    WorkflowBuilder(
      chain(graph, 
        projectOp(Reshape.Doc(ListMap(BsonField.Name(name) -> -\/ (base))))),
      DocVar.ROOT(),
      struct.makeObject(name))

  def makeArray: WorkflowBuilder =
    copy(
      graph = chain(graph, projectOp(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/ (base))))),
      base = DocVar.ROOT(),
      struct = struct.makeArray(0))

  def objectConcat(that: WorkflowBuilder): Error \/ WorkflowBuilder = {
    import SchemaChange._

    def mergeUnknownSchemas(entries: List[Js.Expr => Js.Stmt]) =
      Js.Call(
        Js.AnonFunDecl(List("rez"),
          entries.map(_(Js.Ident("rez"))) :+ Js.Return(Js.Ident("rez"))),
        List(Js.AnonObjDecl(Nil)))

    this.merge(that) { (left, right, list) =>
        def builderWithUnknowns(
          src: WorkflowOp, base: String, fields: List[Js.Expr => Js.Stmt]) =
          WorkflowBuilder(
            chain(src, mapOp(MapOp.mapMap(base, mergeUnknownSchemas(fields)))),
            DocVar.ROOT(),
            Init)

        (this.struct.simplify, that.struct.simplify) match {
          case (MakeObject(m1), MakeObject(m2)) =>
            def convert(root: DocVar) = (keys: Seq[String]) =>
              keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

            \/-(WorkflowBuilder(
              chain(list,
                projectOp(Reshape.Doc(ListMap((convert(left)(m1.keys.toSeq) ++ convert(right)(m2.keys.toSeq)): _*)))),
              DocVar.ROOT(),
              MakeObject(m1 ++ m2)))
          case (Init, MakeObject(m)) =>
            \/-(builderWithUnknowns(
              list,
              "leftUnknown",
              List(ReduceOp.copyAllFields((fromDocVar(left)).toJs(Js.Ident("leftUnknown")))) ++
                m.toList.map { case (k, v) =>
                  ReduceOp.copyOneField(Js.Access(_, Js.Str(k)), (fromDocVar(right \ BsonField.Name(k))).toJs(Js.Ident("leftUnknown")))
                }))
          case (MakeObject(m), Init) =>
            \/-(builderWithUnknowns(
              list,
              "rightUnknown",
              m.toList.map { case (k, v) =>
                ReduceOp.copyOneField(Js.Access(_, Js.Str(k)), (fromDocVar(left \ BsonField.Name(k))).toJs(Js.Ident("rightUnknown")))
              } ++
                List(ReduceOp.copyAllFields((fromDocVar(right)).toJs(Js.Ident("rightUnknown"))))))
          case (Init, Init) =>
            \/-(builderWithUnknowns(
              list,
              "bothUnknown",
              List(
                ReduceOp.copyAllFields((fromDocVar(left)).toJs(Js.Ident("bothUnknown"))),
                ReduceOp.copyAllFields((fromDocVar(right)).toJs(Js.Ident("bothUnknown"))))))
          case (l @ FieldProject(s1, f1), r @ FieldProject(s2, f2)) =>
            def convert(root: DocVar) = (keys: Seq[String]) =>
              keys.map(BsonField.Name.apply).map(name => name -> -\/(root)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

            \/-(builderWithUnknowns(
              chain(list,
                projectOp(Reshape.Doc(ListMap((convert(left)(List(f1)) ++ convert(right)(List(f2))): _*)))),
              "bothProjects",
              List(
                ReduceOp.copyAllFields(l.toJs(Js.Ident("bothProjects"))),
                ReduceOp.copyAllFields(r.toJs(Js.Ident("bothProjects"))))))
          case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
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
            \/- (WorkflowBuilder(
              chain(list, projectOp(
                Reshape.Arr(ListMap((convert(left)(0, m1.keys.toSeq) ++ convert(right)(rightShift, m2.keys.toSeq)): _*)))),
              DocVar.ROOT(),
              SchemaChange.MakeArray(m1 ++ m2.map(t => (t._1 + rightShift) -> t._2))))
          }

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (WorkflowBuilderError.CannotObjectConcatExpr)
    }
  }

  def flattenObject: WorkflowBuilder = {
    val field = base.toJs(Js.Ident("value"))
    copy(
      graph =
        chain(graph,
          flatMapOp(
            Js.AnonFunDecl(List("key", "value"),
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
      base = DocVar.ROOT())
  }

  def flattenArray: WorkflowBuilder =
    copy(graph = chain(graph, unwindOp(base)))

  def projectField(name: String): WorkflowBuilder =
    WorkflowBuilder(
      chain(graph,
        projectOp(Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Name(name)))))),
      ExprVar,
      struct.projectField(name))
    
  def projectIndex(index: Int): WorkflowBuilder =
    copy(
      // TODO: Replace the map/reduce with this projection when
      //       https://jira.mongodb.org/browse/SERVER-4589 is fixed
      // graph = ProjectOp(graph, Reshape.Doc(ListMap(
      //   ExprName -> -\/ (base \ BsonField.Index(index))))),
      graph = chain(graph,
        mapOp(
          MapOp.mapMap("value",
            Js.Access(
              base.toJs(Js.Ident("value")),
              Js.Num(index, false))))),
      base = DocVar.ROOT(),
      struct = struct.projectIndex(index))

  def groupBy(that: WorkflowBuilder): Error \/ WorkflowBuilder =
    (this merge that) { (value, key, src) =>
      \/- (WorkflowBuilder(
        chain(src,
          groupOp(
            Grouped(ListMap(
              ExprName -> ExprOp.Push(value))),
              -\/ (key))),
        ExprVar,
        this.struct.projectField(ExprLabel)))
    }

  def reduce(f: ExprOp => ExprOp.GroupOp): Error \/ WorkflowBuilder = {
    val reduced = (graph, base) match {
      case (GroupOp(src, Grouped(values), by), ExprOp.DocVar(_, Some(name @ BsonField.Name(_)))) =>
        values.get(name) match {
          case Some(ExprOp.Push(expr)) => 
            Some(WorkflowBuilder(
              chain(src,
                groupOp(
                  Grouped(values + (ExprName -> f(expr))),
                  by)),
              ExprVar,
              this.struct.projectField(ExprLabel)))
          
          case _ => 
            None
        }
      
      case _ => None
    }
    reduced match {
      case Some(wb) => \/- (wb)
      case None => for {
        grouped <- this.groupBy(WorkflowBuilder.pure(Bson.Null))
        reduced <- grouped.reduce(f)
      } yield reduced
    }
  }

  def sortBy(that: WorkflowBuilder, sortTypes: List[SortType]):
      Error \/ WorkflowBuilder = {
    val flat = copy(graph = flattenGrouped(graph))
    flat.merge(that) { (sort, by, list) =>
      (that.struct.simplify, by) match {
        case (SchemaChange.MakeArray(els), DocVar(_, Some(by))) =>
          if (els.size != sortTypes.length) -\/ (WorkflowBuilderError.InvalidSortBy)
          else {
            val sortFields = (els.zip(sortTypes).foldLeft(List.empty[(BsonField, SortType)]) {
              case (acc, ((idx, s), sortType)) =>
                val index = BsonField.Index(idx)

                val key: BsonField = by \ index

                (key -> sortType) :: acc
            }).reverse

            sortFields match {
              case Nil => -\/ (WorkflowBuilderError.InvalidSortBy)

              case x :: xs => 
                \/- (WorkflowBuilder(
                  chain(list, 
                    sortOp(NonEmptyList.nel(x, xs))),
                  sort,
                  self.struct))
            }
          }

        case _ => -\/ (WorkflowBuilderError.InvalidSortBy)
      }
    }
  }

  def join(that: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: ExprOp, rightKey: Js.Expr => Js.Expr):
      Error \/ WorkflowBuilder = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._
    import PipelineOp._

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
          rightField -> -\/(r)))))
    }

    def buildJoin(src: WorkflowOp, tpe: JoinType): SingleSourceOp =
      tpe match {
        case FullOuter => 
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case LeftOuter =>           
          chain(src,
            matchOp(Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(ExprOp.DocField(leftField), padEmpty(rightField)))
        case RightOuter =>           
          chain(src,
            matchOp(Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), ExprOp.DocField(rightField)))
        case Inner =>
          chain(
            src,
            matchOp(
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
      }

    def rightMap(keyExpr: Expr => Expr): AnonFunDecl =
      MapOp.mapKeyVal(("key", "value"),
        keyExpr(Ident("value")),
        AnonObjDecl(List(
          ("left", AnonElem(Nil)),
          ("right", AnonElem(List(Ident("value")))))))

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
                groupOp(
                  Grouped(ListMap(
                    leftField -> ExprOp.AddToSet(ExprOp.DocVar.ROOT()))),
                  -\/(leftKey)),
                WorkflowOp.projectOp(
                  Reshape.Doc(ListMap(
                    leftField -> -\/(DocVar.ROOT(leftField)),
                    rightField -> -\/(ExprOp.Literal(Bson.Arr(Nil))))),
                  IncludeId)),
              chain(that.graph,
                mapOp(rightMap(rightKey)),
                reduceOp(rightReduce))),
            buildJoin(_, tpe),
            unwindOp(ExprOp.DocField(leftField)),
            unwindOp(ExprOp.DocField(rightField))),
          DocVar.ROOT(),
          SchemaChange.Init))
      case _ => -\/(WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(that: WorkflowBuilder) =
    this.join(that,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      ExprOp.Literal(Bson.Null), Function.const(Js.Null))

  def >>> (op: WorkflowOp => WorkflowOp): WorkflowBuilder = {
    val (newGraph, newBase) = WorkflowOp.rewrite(op(graph), base)
    copy(graph = newGraph, base = newBase)
  }

  def squash: WorkflowBuilder = this

  def distinctBy(key: WorkflowBuilder): Error \/ WorkflowBuilder = {
    def sortKeys(op: WorkflowOp): Error \/ List[(BsonField, SortType)] = {
      object HavingRoot {
        def unapply(shape: Reshape): Option[BsonField] = shape match {
          case Reshape.Doc(map) => map.collect { case (name,  -\/ (op)) if op == ExprOp.DocVar.ROOT() => name  }.headOption
          case Reshape.Arr(map) => map.collect { case (index, -\/ (op)) if op == ExprOp.DocVar.ROOT() => index }.headOption
        }
      }

      def isOrdered(op: WorkflowOp): Boolean = op match {
        case SortOp(_, _)     => true
        case GroupOp(_, _, _) => false
        case _: GeoNearOp     => true
        case p: WPipelineOp => isOrdered(p.src)
        case _ => false
      }

      // Note: this currently only handles a couple of cases, which are the ones
      // that are generated by the compiler for SQL's distinct keyword, with
      // order by, with or without "synthetic" projections. A more general
      // implementation would rewrite the pipeline to handle additional cases.
      op match {
        case SortOp(_, keys) => \/- (keys.list)

        case ProjectOp(SortOp(_, keys), HavingRoot(root), _) =>
          \/- (keys.list.map {
            case (field, sortType) => (root \ field) -> sortType
          })

        case _ =>
          if (isOrdered(op)) -\/ (WorkflowBuilderError.UnsupportedDistinct("cannot distinct with unrecognized ordered source: " + op))
          else \/- (Nil)
      }
    }

    val lFlat = this.copy(graph=flattenGrouped(this.graph))
    val rFlat = key.copy(graph=flattenGrouped(key.graph))
    lFlat.merge(rFlat) { (value, by, merged) =>
      sortKeys(merged).flatMap { sk =>
        val keyPrefix = "__sd_key_"
        val keyProjs = sk.zipWithIndex.map { case ((name, _), index) => BsonField.Name(keyPrefix + index.toString) -> ExprOp.First(ExprOp.DocField(name)) }
        val values = ExprName -> ExprOp.First(value) :: keyProjs

        val group = (key.struct.simplify, by) match {
          case (_, DocVar(_, Some(_))) =>
            \/- (chain(merged,
                groupOp(
                  Grouped(ListMap(values: _*)),
                  -\/ (by))))

          // If the key is at the document root, we must explicitly project out the fields
          // so as not to include a meaningless _id in the key:
          case (SchemaChange.MakeObject(byFields), _) =>
            \/- (chain(merged,
                groupOp(
                  Grouped(ListMap(values: _*)),
                  \/- (Reshape.Arr(ListMap(
                    byFields.keys.toList.zipWithIndex.map { case (name, index) => 
                      BsonField.Index(index) -> -\/ (by \ BsonField.Name(name)) 
                    }: _*))))))
          
          case (SchemaChange.MakeArray(byFields), _) =>
            \/- (chain(merged,
                groupOp(
                  Grouped(ListMap(values: _*)),
                  \/- (Reshape.Arr(ListMap(
                    byFields.keys.toList.map { index =>
                      BsonField.Index(index) -> -\/ (by \ BsonField.Index(index))
                    }: _*))))))

          case _ => -\/ (WorkflowBuilderError.UnsupportedDistinct("Cannot distinct with unknown shape (" + struct + "; " + key.struct + "; " + by + ")"))
        }

        group.map { op =>
          val sorted =
            if (sk.isEmpty) op
            else {
              val keyPairs = sk.zipWithIndex.map { case ((name, sortType), index) => BsonField.Name(keyPrefix + index.toString) -> sortType }
              keyPairs.headOption.map { head =>
                val tail = keyPairs.drop(1)
                chain(op,
                  sortOp(NonEmptyList(head, tail: _*)))
              }.getOrElse(op)
            }

          WorkflowBuilder(
            sorted,
            ExprVar,
            this.struct)
        }
      }
    }
  }

  def asExprOp = this match {
    case WorkflowBuilder(ProjectOp(_, _, IdHandling.IncludeId), _, _) => None
    case WorkflowBuilder(ProjectOp(_, Reshape.Doc(fields), _), `ExprVar`, _) =>
      fields.toList match {
        case List((ExprName, -\/(e))) => Some(e)
        case _                        => None
      }
    case WorkflowBuilder(PureOp(bson), _, _) => Some(ExprOp.Literal(bson))
    case _ => None
  }

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
  import IdHandling._

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
      chain(src,
        projectOp(
          PipelineOp.Reshape.Doc(ListMap(ExprName -> -\/ (expr))),
          IgnoreId)),
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
        Nil,
      "WorkflowBuilder" :: Nil)
  }
}
