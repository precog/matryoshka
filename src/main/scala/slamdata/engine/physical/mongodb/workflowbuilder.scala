package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import slamdata.engine.fp._
import slamdata.engine.fs.Path
import slamdata.engine._
import Workflow._
import slamdata.engine.analysis.fixplate._
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
  case class InvalidGroup[A](op: WorkflowF[A]) extends WorkflowBuilderError {
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
  graph: Workflow,
  base: ExprOp.DocVar,
  struct: SchemaChange) { self =>
  import WorkflowBuilder._
  import Workflow._
  import ExprOp.{DocVar}
  import IdHandling._

  def build: Workflow = base match {
    case DocVar.ROOT(None) => Workflow.finish(flattenGrouped(graph))
    case base =>
      val g1 = struct.shift(base).map(t => chain(graph, Workflow.$project(t._1, t._2))).getOrElse(graph)
      copy(graph = g1, base = DocVar.ROOT()).build
  }

  private def flattenGrouped(op: Workflow): Workflow = {
    val pushed = op.unFix match {
      case $Group(_, Grouped(grouped), _) =>
        grouped.collect {
          case (name, ExprOp.Push(_)) => name
        }

      case _ => Nil
    }
    pushed.foldLeft(op)((op, name) => chain(op, $unwind(ExprOp.DocField(name))))
  }

  private def $project(shape: Reshape): WorkflowOp =
    Workflow.$project(
      shape,
      shape.get(IdName).fold[IdHandling](IgnoreId)(Function.const(IncludeId)))

  def asLiteral = asExprOp.collect { case (x @ ExprOp.Literal(_)) => x }

  def expr1(f: DocVar => ExprOp): MId[WorkflowBuilder] =
    freshName.map { name =>
      val that = WorkflowBuilder.fromExpr(name, graph, f(base))
      copy(graph = that.graph, base = that.base)
    }

  def expr2(that: WorkflowBuilder)(f: (DocVar, DocVar) => ExprOp):
      MId[WorkflowBuilder] =
    freshName.flatMap(name =>
      this.merge(that) { (lbase, rbase, list) =>
        f(lbase, rbase) match {
          case DocVar.ROOT(None) => (this applyLens _graph).set(list)
          case expr =>
            WorkflowBuilder(
              chain(list, $project(Reshape.Doc(ListMap(name -> -\/ (expr))))),
              ExprOp.DocField(name),
              SchemaChange.Init)
        }
      })
  
  def expr3(
    p2: WorkflowBuilder, p3: WorkflowBuilder)(
    f: (DocVar, DocVar, DocVar) => ExprOp):
      MId[WorkflowBuilder] = {
    def nest(lname: BsonField.Name, rname: BsonField.Name) =
      (lbase: DocVar, rbase: DocVar, list: Workflow) =>
        WorkflowBuilder(
          chain(list,
            $project(Reshape.Doc(ListMap(
              lname -> -\/(lbase),
              rname -> -\/(rbase))))),
          DocVar.ROOT(),
          SchemaChange.Init)

    for {
      l      <- freshName
      ll     <- freshName
      lr     <- freshName
      r      <- freshName
      p12    <- this.merge(p2)(nest(ll, lr))
      p123   <- p12.merge(p3)(nest(l, r))
      pfinal <- p123.expr1 { root =>
        f(root \ l \ ll, root \ l \ lr, root \ r)
      }
    } yield pfinal
  }

  def makeObject(name: String): WorkflowBuilder =
    WorkflowBuilder(
      chain(graph, 
        $project(Reshape.Doc(ListMap(BsonField.Name(name) -> -\/ (base))))),
      DocVar.ROOT(),
      struct.makeObject(name))

  def makeArray: WorkflowBuilder =
    copy(
      graph = chain(graph, $project(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/ (base))))),
      base = DocVar.ROOT(),
      struct = struct.makeArray(0))

  def objectConcat(that: WorkflowBuilder): M[WorkflowBuilder] = {
    import SchemaChange._

    def mergeUnknownSchemas(entries: List[Js.Expr => Js.Stmt]) =
      Js.Call(
        Js.AnonFunDecl(List("rez"),
          entries.map(_(Js.Ident("rez"))) :+ Js.Return(Js.Ident("rez"))),
        List(Js.AnonObjDecl(Nil)))

    swapM(this.merge(that) { (left, right, list) =>
      def builderWithUnknowns(
        src: Workflow,
        base: String,
        fields: List[Js.Expr => Js.Stmt]) =
        WorkflowBuilder(
          chain(src, $map($Map.mapMap(base, mergeUnknownSchemas(fields)))),
          DocVar.ROOT(),
          Init)

      (this.struct.simplify, that.struct.simplify) match {
        case (MakeObject(m1), MakeObject(m2)) =>
          def convert(root: DocVar) = (keys: Seq[String]) =>
          keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

          \/-(WorkflowBuilder(
            chain(list,
              $project(Reshape.Doc(ListMap((convert(left)(m1.keys.toSeq) ++ convert(right)(m2.keys.toSeq)): _*)))),
            DocVar.ROOT(),
            MakeObject(m1 ++ m2)))
        case (Init, MakeObject(m)) =>
          \/-(builderWithUnknowns(
            list,
            "leftUnknown",
            List($Reduce.copyAllFields((fromDocVar(left)).toJs(Js.Ident("leftUnknown")))) ++
              m.toList.map { case (k, v) =>
                $Reduce.copyOneField(Js.Access(_, Js.Str(k)), (fromDocVar(right \ BsonField.Name(k))).toJs(Js.Ident("leftUnknown")))
              }))
        case (MakeObject(m), Init) =>
          \/-(builderWithUnknowns(
            list,
            "rightUnknown",
            m.toList.map { case (k, v) =>
              $Reduce.copyOneField(Js.Access(_, Js.Str(k)), (fromDocVar(left \ BsonField.Name(k))).toJs(Js.Ident("rightUnknown")))
            } ++
              List($Reduce.copyAllFields((fromDocVar(right)).toJs(Js.Ident("rightUnknown"))))))
        case (Init, Init) =>
          \/-(builderWithUnknowns(
            list,
            "bothUnknown",
            List(
              $Reduce.copyAllFields((fromDocVar(left)).toJs(Js.Ident("bothUnknown"))),
              $Reduce.copyAllFields((fromDocVar(right)).toJs(Js.Ident("bothUnknown"))))))
        case (l @ FieldProject(s1, f1), r @ FieldProject(s2, f2)) =>
          def convert(root: DocVar) = (keys: Seq[String]) =>
          keys.map(BsonField.Name.apply).map(name => name -> -\/(root)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

          \/-(builderWithUnknowns(
            chain(list,
              $project(Reshape.Doc(ListMap((convert(left)(List(f1)) ++ convert(right)(List(f2))): _*)))),
            "bothProjects",
            List(
              $Reduce.copyAllFields(l.toJs(Js.Ident("bothProjects"))),
              $Reduce.copyAllFields(r.toJs(Js.Ident("bothProjects"))))))
        case _ => -\/(WorkflowBuilderError.CannotObjectConcatExpr)
      }
    })
  }

  def arrayConcat(that: WorkflowBuilder): M[WorkflowBuilder] = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeArray(m1), s2 @ SchemaChange.MakeArray(m2)) =>
        def convert(root: DocVar) = (shift: Int, keys: Seq[Int]) => 
          (keys.map { index => 
            BsonField.Index(index + shift) -> -\/ (root \ BsonField.Index(index))
          }): Seq[(BsonField.Index, ExprOp \/ Reshape)]

        emitSt(this.merge(that) { (left, right, list) =>
          val rightShift = m1.keys.max + 1
          WorkflowBuilder(
            chain(list, $project(
              Reshape.Arr(ListMap((convert(left)(0, m1.keys.toSeq) ++ convert(right)(rightShift, m2.keys.toSeq)): _*)))),
            DocVar.ROOT(),
            SchemaChange.MakeArray(m1 ++ m2.map(t => (t._1 + rightShift) -> t._2)))
        })

      // TODO: Here's where we'd handle Init case

      case _ => fail(WorkflowBuilderError.CannotObjectConcatExpr)
    }
  }

  def flattenObject: WorkflowBuilder = {
    val field = base.toJs(Js.Ident("value"))
    copy(
      graph =
        chain(graph,
          $flatMap(
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
    copy(graph = chain(graph, $unwind(base)))

  def projectField(name: String): WorkflowBuilder =
    WorkflowBuilder(
      chain(graph,
        $project(Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Name(name)))))),
      ExprVar,
      struct.projectField(name))
    
  def projectIndex(index: Int): WorkflowBuilder =
    copy(
      // TODO: Replace the map/reduce with this projection when
      //       https://jira.mongodb.org/browse/SERVER-4589 is fixed
      // graph = $Project(graph, Reshape.Doc(ListMap(
      //   ExprName -> -\/ (base \ BsonField.Index(index))))),
      graph = chain(graph,
        $map(
          $Map.mapMap("value",
            Js.Access(
              base.toJs(Js.Ident("value")),
              Js.Num(index, false))))),
      base = DocVar.ROOT(),
      struct = struct.projectIndex(index))

  def groupBy(that: WorkflowBuilder): MId[WorkflowBuilder] =
    freshName.flatMap(name =>
      (this merge that) { (value, key, src) =>
        WorkflowBuilder(
          chain(src,
            $group(Grouped(ListMap(name -> ExprOp.Push(value))), -\/(key))),
          ExprOp.DocField(name),
          this.struct.projectField(ExprLabel))
      })

  def reduce(f: ExprOp => ExprOp.GroupOp): MId[WorkflowBuilder] = {
    val reduced = (graph.unFix, base) match {
      case ($Group(src, Grouped(values), by), ExprOp.DocVar(_, Some(name @ BsonField.Name(_)))) =>
        values.get(name) match {
          case Some(ExprOp.Push(expr)) => 
            Some(WorkflowBuilder(
              chain(src,
                $group(
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
      case Some(wb) => wb.point[MId]
      case None     => this.groupBy(pure(Bson.Null)).flatMap(_.reduce(f))
    }
  }

  def sortBy(that: WorkflowBuilder, sortTypes: List[SortType]):
      M[WorkflowBuilder] = {
    val flat = copy(graph = flattenGrouped(graph))
    swapM(flat.merge(that) { (sort, by, list) =>
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
              case Nil => -\/(WorkflowBuilderError.InvalidSortBy)

              case x :: xs =>
                \/-(WorkflowBuilder(
                  chain(list,
                    $sort(NonEmptyList.nel(x, xs))),
                  sort,
                  self.struct))
            }
          }

        case _ => -\/(WorkflowBuilderError.InvalidSortBy)
      }
    })
  }

  def join(that: WorkflowBuilder,
    tpe: slamdata.engine.LogicalPlan.JoinType, comp: Mapping,
    leftKey: ExprOp, rightKey: Js.Expr => Js.Expr):
      Error \/ WorkflowBuilder = {

    import slamdata.engine.LogicalPlan.JoinType
    import slamdata.engine.LogicalPlan.JoinType._
    import Js._

    // Note: these have to match the names used in the logical plan
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

    def buildProjection(l: ExprOp, r: ExprOp):
        WorkflowOp =
      chain(_,
        $project(Reshape.Doc(ListMap(
          leftField -> -\/(l),
          rightField -> -\/(r)))))

    def buildJoin(src: Workflow, tpe: JoinType): Workflow =
      tpe match {
        case FullOuter => 
          chain(src,
            buildProjection(padEmpty(leftField), padEmpty(rightField)))
        case LeftOuter =>           
          chain(src,
            $match(Selector.Doc(ListMap(
              leftField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(ExprOp.DocField(leftField), padEmpty(rightField)))
        case RightOuter =>           
          chain(src,
            $match(Selector.Doc(ListMap(
              rightField.asInstanceOf[BsonField] -> nonEmpty))),
            buildProjection(padEmpty(leftField), ExprOp.DocField(rightField)))
        case Inner =>
          chain(
            src,
            $match(
              Selector.Doc(ListMap(
                leftField.asInstanceOf[BsonField] -> nonEmpty,
                rightField -> nonEmpty))))
      }

    def rightMap(keyExpr: Expr => Expr): AnonFunDecl =
      $Map.mapKeyVal(("key", "value"),
        keyExpr(Ident("value")),
        AnonObjDecl(List(
          (leftField.asText, AnonElem(Nil)),
          (rightField.asText, AnonElem(List(Ident("value")))))))

    val rightReduce =
      AnonFunDecl(List("key", "values"),
        List(
          VarDef(List(("result",
            AnonObjDecl(List(
              (leftField.asText, AnonElem(Nil)),
              (rightField.asText, AnonElem(Nil))))))),
          Call(Select(Ident("values"), "forEach"),
            List(AnonFunDecl(List("value"),
              // TODO: replace concat here with a more efficient operation
              //      (push or unshift)
              List(
                BinOp("=",
                  Select(Ident("result"), leftField.asText),
                  Call(Select(Select(Ident("result"), leftField.asText), "concat"),
                    List(Select(Ident("value"), leftField.asText)))),
                BinOp("=",
                  Select(Ident("result"), rightField.asText),
                  Call(Select(Select(Ident("result"), rightField.asText), "concat"),
                    List(Select(Ident("value"), rightField.asText)))))))),
          Return(Ident("result"))))

    comp match {
      case relations.Eq =>
        \/- (WorkflowBuilder(
          chain(
            $foldLeft(
              chain(
                this.graph,
                $group(
                  Grouped(ListMap(
                    leftField -> ExprOp.AddToSet(ExprOp.DocVar.ROOT()))),
                  -\/(leftKey)),
                Workflow.$project(
                  Reshape.Doc(ListMap(
                    leftField -> -\/(DocVar.ROOT(leftField)),
                    rightField -> -\/(ExprOp.Literal(Bson.Arr(Nil))))),
                  IncludeId)),
              chain(that.graph,
                $map(rightMap(rightKey)),
                $reduce(rightReduce))),
            buildJoin(_, tpe),
            $unwind(ExprOp.DocField(leftField)),
            $unwind(ExprOp.DocField(rightField))),
          DocVar.ROOT(),
          SchemaChange.Init))
      case _ => -\/ (WorkflowBuilderError.UnsupportedJoinCondition(comp))
    }
  }

  def cross(that: WorkflowBuilder) =
    this.join(that,
      slamdata.engine.LogicalPlan.JoinType.Inner, relations.Eq,
      ExprOp.Literal(Bson.Null), Function.const(Js.Null))

  def >>> (op: WorkflowOp): WorkflowBuilder = {
    val (newGraph, newBase) = Workflow.rewrite(op(graph).unFix, base)
    copy(graph = Term(newGraph), base = newBase)
  }

  def squash: WorkflowBuilder = this

  def distinctBy(key: WorkflowBuilder): M[WorkflowBuilder] = {
    def sortKeys(op: Workflow): Error \/ List[(BsonField, SortType)] = {
      object HavingRoot {
        def unapply(shape: Reshape): Option[BsonField] = shape match {
          case Reshape.Doc(map) => map.collect { case (name,  -\/ (op)) if op == ExprOp.DocVar.ROOT() => name  }.headOption
          case Reshape.Arr(map) => map.collect { case (index, -\/ (op)) if op == ExprOp.DocVar.ROOT() => index }.headOption
        }
      }

      def isOrdered(op: Workflow): Boolean = op.unFix match {
        case $Sort(_, _)                            => true
        case $Group(_, _, _)                        => false
        case $GeoNear(_, _, _, _, _, _, _, _, _, _) => true
        case p: PipelineF[_]                         => isOrdered(p.src)
        case _                                       => false
      }

      // Note: this currently only handles a couple of cases, which are the ones
      // that are generated by the compiler for SQL's distinct keyword, with
      // order by, with or without "synthetic" projections. A more general
      // implementation would rewrite the pipeline to handle additional cases.
      op.unFix match {
        case $Sort(_, keys) => \/- (keys.list)

        case $Project(Term($Sort(_, keys)), HavingRoot(root), _) =>
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
    swapM(lFlat.merge(rFlat) { (value, by, merged) =>
      sortKeys(merged).flatMap { sk =>
        val keyPrefix = "__sd_key_"
        val keyProjs = sk.zipWithIndex.map { case ((name, _), index) => BsonField.Name(keyPrefix + index.toString) -> ExprOp.First(ExprOp.DocField(name)) }
        val values = ExprName -> ExprOp.First(value) :: keyProjs

        val group = (key.struct.simplify, by) match {
          case (_, DocVar(_, Some(_))) =>
            \/- (chain(merged,
              $group(
                Grouped(ListMap(values: _*)),
                -\/ (by))))

          // If the key is at the document root, we must explicitly project out
          // the fields so as not to include a meaningless _id in the key:
          case (SchemaChange.MakeObject(byFields), _) =>
            \/- (chain(merged,
              $group(
                Grouped(ListMap(values: _*)),
                \/- (Reshape.Arr(ListMap(
                  byFields.keys.toList.zipWithIndex.map { case (name, index) =>
                    BsonField.Index(index) -> -\/ (by \ BsonField.Name(name))
                  }: _*))))))

          case (SchemaChange.MakeArray(byFields), _) =>
            \/- (chain(merged,
              $group(
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
                      $sort(NonEmptyList(head, tail: _*)))
                }.getOrElse(op)
            }

          WorkflowBuilder(
            sorted,
            ExprVar,
            this.struct)
        }
      }
    })
  }

  def asExprOp = (graph.unFix, base) match {
    case ($Project(_, _, IdHandling.IncludeId), _) => None
    case ($Project(_, Reshape.Doc(fields), _), ExprVar) =>
      fields.toList match {
        case List((ExprName, -\/(e))) => Some(e)
        case _                        => None
      }
    case ($Pure(bson), _) => Some(ExprOp.Literal(bson))
    case _ => None
  }

  private def merge[A](
    that: WorkflowBuilder)(f: (DocVar, DocVar, Workflow) => A):
      State[NameGen, A] =
    (Workflow.merge(this.graph, that.graph)).map { case ((lbase, rbase), op) =>
      f(lbase \\ this.base, rbase \\ that.base, op)
    }
}

object WorkflowBuilder {
  import Workflow._
  import ExprOp.{DocVar}
  import IdHandling._

  type EitherE[X] = Error \/ X
  type M[X] = StateT[EitherE, NameGen, X]
  type MId[X] = State[NameGen, X]

  // Wrappers for results that don't use state:
  def emit[A](a: A): M[A] = lift(\/- (a))
  def fail[A](e: Error): M[A] = lift(-\/ (e))
  def lift[A](v: Error \/ A): M[A] = StateT[EitherE, NameGen, A](s => v.map(s -> _))
  
  // Wrappers for results that don't fail:
  def emitSt[A](v: State[NameGen, A]): M[A] =
    StateT[EitherE, NameGen, A](s => \/-(v.run(s)))

  def swapM[A](v: State[NameGen, Error \/ A]): StateT[EitherE, NameGen, A] =
    StateT[EitherE, NameGen, A](s => { val (s1, x) = v.run(s); x.map(s1 -> _) })

  def read(coll: Collection) =
    WorkflowBuilder($read(coll), DocVar.ROOT(), SchemaChange.Init)
  def pure(bson: Bson) =
    WorkflowBuilder($pure(bson), DocVar.ROOT(), SchemaChange.Init)

  def fromExpr(name: BsonField.Name, src: Workflow, expr: ExprOp): WorkflowBuilder =
    WorkflowBuilder(
      chain(src,
        $project(Reshape.Doc(ListMap(name -> -\/ (expr))), IgnoreId)),
      ExprOp.DocField(name),
      SchemaChange.Init)

  val _graph  = mkLens[WorkflowBuilder, Workflow]("graph")
  val _base   = mkLens[WorkflowBuilder, DocVar]("base")
  val _struct = mkLens[WorkflowBuilder, SchemaChange]("struct")
  
  implicit def WorkflowBuilderRenderTree(implicit RO: RenderTree[Workflow], RE: RenderTree[ExprOp]): RenderTree[WorkflowBuilder] = new RenderTree[WorkflowBuilder] {
    def render(v: WorkflowBuilder) = NonTerminal("",
      RO.render(v.graph) ::
        RE.render(v.base) ::
        Terminal(v.struct.toString, "WorkflowBuilder" :: "SchemaChange" :: Nil) ::
        Nil,
      "WorkflowBuilder" :: Nil)
  }
}
