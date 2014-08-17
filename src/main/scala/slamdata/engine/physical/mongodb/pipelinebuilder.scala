package slamdata.engine.physical.mongodb

import collection.immutable.ListMap

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, Error}
import slamdata.engine.fp._

sealed trait PipelineBuilderError extends Error
object PipelineBuilderError {
  case object CouldNotPatchRoot extends PipelineBuilderError {
    def message = "Could not patch ROOT"
  }
  case object CannotObjectConcatExpr extends PipelineBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object CannotArrayConcatExpr extends PipelineBuilderError {
    def message = "Cannot array concat an expression"
  }
  case object NotGrouped extends PipelineBuilderError {
    def message = "The pipeline builder has not been grouped by another set, so a group op doesn't make sense"
  }
  case object InvalidSortBy extends PipelineBuilderError {
    def message = "The sort by set has an invalid structure"
  }
  case object UnknownStructure extends PipelineBuilderError {
    def message = "The structure is unknown due to a missing project or group operation"
  }
}

/**
 * A `PipelineBuilder` consists of a list of pipeline operations in *reverse*
 * order, a structure, and a base mod for that structure.
 */
final case class PipelineBuilder private (buffer: List[PipelineOp], base: ExprOp.DocVar, struct: SchemaChange, groupBy: List[PipelineBuilder]) { self =>
  import PipelineBuilder._
  import PipelineOp._
  import ExprOp.{DocVar, GroupOp}

  def build: Error \/ Pipeline = {
    base match {
      case DocVar.ROOT(None) => \/- (Pipeline(simplify.buffer.reverse))

      case base =>
        struct match {
          case s @ SchemaChange.MakeObject(_) => 
            \/- (Pipeline(copy(buffer = s.shift(base) :: buffer, base = DocVar.ROOT()).simplify.buffer.reverse))

          case s @ SchemaChange.MakeArray(_) =>
            \/- (Pipeline(copy(buffer = s.shift(base) :: buffer, base = DocVar.ROOT()).simplify.buffer.reverse))

          case _ => 
            // println(simplify.buffer.reverse.shows)

            -\/ (PipelineBuilderError.UnknownStructure)
        }
    }
  }

  def simplify: PipelineBuilder = copy(buffer = Project.simplify(buffer.reverse).reverse)

  def isExpr = asExprOp.isDefined

  def asLiteral = asExprOp.collect {
    case (x @ ExprOp.Literal(_)) => x
  }

  def expr1(f: DocVar => Error \/ ExprOp): Error \/ PipelineBuilder = f(base).map { expr =>
    val that = PipelineBuilder.fromExpr(expr)

    copy(buffer = that.buffer ::: this.buffer, base = that.base)
  }

  def expr2(that: PipelineBuilder)(f: (DocVar, DocVar) => Error \/ ExprOp): Error \/ PipelineBuilder = {
    this.merge(that) { (lbase, rbase, list) =>
      f(lbase, rbase).flatMap { 
        case DocVar.ROOT(None) => \/- (copy(buffer = list))

        case expr => mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
          new PipelineBuilder(
            buffer  = Project(Reshape.Doc(ListMap(ExprName -> -\/ (expr)))) :: list,
            base    = ExprVar,
            struct  = SchemaChange.Init,
            groupBy = mergedGroups
          )
        }
      }
    }
  }

  def expr3(p2: PipelineBuilder, p3: PipelineBuilder)(f: (DocVar, DocVar, DocVar) => Error \/ ExprOp): Error \/ PipelineBuilder = {
    val nest = (lbase: DocVar, rbase: DocVar, list: List[PipelineOp]) => {
      val p = Project(Reshape.Doc(ListMap(LeftName -> -\/ (lbase), RightName -> -\/ (rbase))))

      mergeGroups(this.groupBy, p2.groupBy, p3.groupBy).map { mergedGroups =>
        new PipelineBuilder(
          buffer  = p :: list,
          base    = DocVar.ROOT(),
          struct  = SchemaChange.Init,
          groupBy = mergedGroups
        )
      }
    }

    (this, p2, p3) match { 
      case (p1, p2, p3) =>
        for {
          p12     <-  p1.merge(p2)(nest)
          p123    <-  p12.merge(p3)(nest)
          pfinal  <-  p123.expr1 { root => 
                        f(root \ LeftName \ LeftName, root \ LeftName \ RightName, root \ RightName)
                      }
        } yield pfinal
    }
  }

  def makeObject(name: String): Error \/ PipelineBuilder = {
    asExprOp.collect {
      case x : GroupOp => 
        groupBy match {
          case Nil => -\/ (PipelineBuilderError.NotGrouped)

          case b :: bs =>
            val (construct, inner) = GroupOp.decon(x)

            val rewritten = copy(
              buffer  = Project(Reshape.Doc(ListMap(ExprName -> -\/ (inner)))) :: buffer.tail
            )

            rewritten.merge(b) { (grouped, by, list) =>
              \/- (new PipelineBuilder(
                buffer  = Group(Grouped(ListMap(BsonField.Name(name) -> construct(grouped))), -\/ (by)) :: list,
                base    = DocVar.ROOT(),
                struct  = self.struct.makeObject(name),
                groupBy = bs
              ))
            }
        }
    }.getOrElse {
      \/- {
        copy(
          buffer  = Project(Reshape.Doc(ListMap(BsonField.Name(name) -> -\/ (base)))) :: buffer, 
          base    = DocVar.ROOT(),
          struct  = struct.makeObject(name)
        )
      }
    }
  }

  def makeArray: Error \/ PipelineBuilder = {
    // TODO: Grouping
    \/- {
      copy(
        buffer  = Project(Reshape.Arr(ListMap(BsonField.Index(0) -> -\/ (base)))) :: buffer, 
        base    = DocVar.ROOT(),
        struct  = struct.makeArray(0)
      )
    }
  }

  def objectConcat(that: PipelineBuilder): Error \/ PipelineBuilder = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeObject(m1), s2 @ SchemaChange.MakeObject(m2)) =>
        def convert(root: DocVar) = (keys: Seq[String]) => 
          keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name)): Seq[(BsonField.Name, ExprOp \/ Reshape)]

        for {
          rez <-  this.merge(that) { (left, right, list) =>
                    val leftTuples  = convert(left)(m1.keys.toSeq)
                    val rightTuples = convert(right)(m2.keys.toSeq)

                    mergeGroups(this.groupBy, that.groupBy).map { mergedGroups =>
                      new PipelineBuilder(
                        buffer  = Project(Reshape.Doc(ListMap((leftTuples ++ rightTuples): _*))) :: list,
                        base    = DocVar.ROOT(),
                        struct  = SchemaChange.MakeObject(m1 ++ m2),
                        groupBy = mergedGroups
                      )
                    }
                  }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (PipelineBuilderError.CannotObjectConcatExpr)
    }
  }

  def arrayConcat(that: PipelineBuilder): Error \/ PipelineBuilder = {
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
                      new PipelineBuilder(
                        buffer  = Project(Reshape.Arr(ListMap((leftTuples ++ rightTuples): _*))) :: list,
                        base    = DocVar.ROOT(),
                        struct  = SchemaChange.MakeArray(m1 ++ m2.map(t => (t._1 + rightShift) -> t._2)),
                        groupBy = mergedGroups
                      )
                    }
                  }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (PipelineBuilderError.CannotObjectConcatExpr)
    }
  }

  def flattenArray: Error \/ PipelineBuilder = \/- (copy(buffer = Unwind(base) :: buffer))

  def projectField(name: String): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Name(name))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectField(name)
      )
    }

  def projectIndex(index: Int): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(ListMap(ExprName -> -\/ (base \ BsonField.Index(index))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectIndex(index)
      )
    }

  def groupBy(that: PipelineBuilder): Error \/ PipelineBuilder = {
    \/- (copy(groupBy = that :: groupBy))
  }

  def reduce(f: ExprOp => GroupOp): Error \/ PipelineBuilder = {
    // TODO: Currently we cheat and defer grouping until we makeObject / 
    //       makeArray. Alas that's not guaranteed and we should find a 
    //       more reliable way.
    expr1(e => \/- (f(e)))
  }

  def isGrouped = !groupBy.isEmpty

  def sortBy(that: PipelineBuilder, sortTypes: List[SortType]): Error \/ PipelineBuilder = {
    this.merge(that) { (sort, by, list) =>
      (that.struct.simplify, by) match {
        case (SchemaChange.MakeArray(els), DocVar(_, Some(by))) =>
          if (els.size != sortTypes.length) -\/ (PipelineBuilderError.InvalidSortBy)
          else {
            val sortFields = (els.zip(sortTypes).foldLeft(List.empty[(BsonField, SortType)]) {
              case (acc, ((idx, s), sortType)) =>
                val index = BsonField.Index(idx)

                val key: BsonField = by \ index \ BsonField.Name("key")

                (key -> sortType) :: acc
            }).reverse

            sortFields match {
              case Nil => -\/ (PipelineBuilderError.InvalidSortBy)

              case x :: xs => 
                mergeGroups(this.groupBy, that.groupBy).map { mergedGroups => // ???
                  new PipelineBuilder(
                    buffer  = Sort(NonEmptyList.nel(x, xs)) :: list,
                    base    = sort,
                    struct  = self.struct,
                    groupBy = mergedGroups
                  )
                }
            }
          }

        case _ => 
          -\/ (PipelineBuilderError.InvalidSortBy)
      }
    }
  }

  def &&& (op: ShapePreservingOp): Error \/ PipelineBuilder =
    // FIXME: this should work more like a merge
    \/-(copy(buffer = buffer :+ op))

  def <<< (op: ShapePreservingOp): Error \/ PipelineBuilder =
    \/-(copy(buffer = buffer :+ op))

  def >>> (op: ShapePreservingOp): Error \/ PipelineBuilder =
    \/-(copy(buffer = op :: buffer))

  def squash: Error \/ PipelineBuilder = {
    if (buffer.collect { case Unwind(_) => () }.isEmpty) \/- (this)
    else {
      val _Id = BsonField.Name("_id")

      struct match {
        case s @ SchemaChange.MakeObject(_) => 
          \/- (copy(buffer = s.shift(base).set(_Id, -\/ (ExprOp.Exclude)) :: buffer, base = DocVar.ROOT()))

        case s @ SchemaChange.MakeArray(_) =>
          \/- (copy(buffer = s.shift(base).set(_Id, -\/ (ExprOp.Exclude)) :: buffer, base = DocVar.ROOT()))

        case _ => -\/ (PipelineBuilderError.UnknownStructure)
      }
    }
  }

  private def asExprOp = this match {
    case PipelineBuilder(Project(Reshape.Doc(fields)) :: _, `ExprVar`, _, _) => 
      fields.toList match {
        case (`ExprName`, -\/ (e)) :: Nil => Some(e)
        case _ => None
      }

    case _ => None
  }

  private def get(f: BsonField): Option[ExprOp \/ Reshape] = {
    val projects = buffer.collect {
      case g @ Group(_, _) => g.toProject
      case p @ Project(_)  => p
    }

    Project.get0(f.flatten, projects.map(_.shape))
  }

  private def merge[A](that: PipelineBuilder)(f: (DocVar, DocVar, List[PipelineOp]) => Error \/ A): Error \/ A = {
    import \&/._
    import cogroup.{Instr, ConsumeLeft, ConsumeRight, ConsumeBoth}

    type Out = Error \/ ((DocVar, DocVar), Instr[PipelineOp])

    def rewrite(op: PipelineOp, base: DocVar): (PipelineOp, DocVar) = {
      (op.rewriteRefs(PartialFunction(base \\ _))) -> (op match { 
        case _ : Group   => DocVar.ROOT() 
        case _ : Project => DocVar.ROOT() 
        
        case _ => base
      })
    }  

    lazy val step: ((DocVar, DocVar), PipelineOp \&/ PipelineOp) => Out = {
      case ((lbase, rbase), these) =>
        def delegate = step((rbase, lbase), these.swap).map {
          case ((rbase, lbase), instr) => ((lbase, rbase), instr.flip)
        }

        val these2 = these.fold[(PipelineOp, DocVar) \&/ (PipelineOp, DocVar)](
          left  => This(rewrite(left,  lbase)),
          right => That(rewrite(right, rbase)),
          (left, right) => Both(rewrite(left,  lbase), 
                                rewrite(right, rbase))
        )

        def consumeLeft(lbase: DocVar, rbase: DocVar)(op: PipelineOp) : Out = {
          \/- ((lbase, rbase) -> ConsumeLeft(op :: Nil))
        }

        def consumeRight(lbase: DocVar, rbase: DocVar)(op: PipelineOp) : Out = {
          \/- ((lbase, rbase) -> ConsumeRight(op :: Nil))
        }

        def consumeBoth(lbase: DocVar, rbase: DocVar)(ops: List[PipelineOp]) : Out = {
          \/- ((lbase, rbase) -> ConsumeBoth(ops))
        }

        these2 match {
          case This((left : ShapePreservingOp, lbase)) => 
            consumeLeft(lbase, rbase)(left)

          case This((left, lbase)) => 
            // Need to preserve all structure on the right hand side, since there are no
            // more operations on the right but there are more on the left.
            val right = Project(Reshape.Doc(ListMap(RightName -> -\/ (rbase))))

            // cogroup really isn't powerful enough here so we "peek ahead" to see if left will
            // be consumed, and if so, change our mind about how to self-delegate.
            step((lbase, DocVar.ROOT()), Both(left, right)).flatMap {
              case ((lbase, _), instr @ ConsumeLeft(_)) => step((lbase, rbase), Both(left, right))
              case ((lbase, rbase), instr) => \/- (((lbase, rbase \ RightName), instr))
            }

          case That(_) => delegate

          case Both((left : GeoNear, lbase), _) =>
            consumeLeft(lbase, rbase)(left)

          case Both(_, (_ : GeoNear, _)) => delegate

          case Both((left : ShapePreservingOp, lbase), _) => 
            consumeLeft(lbase, rbase)(left)

          case Both(_, right : ShapePreservingOp) => delegate

          case Both((Group(Grouped(g1_), b1), lbase), (Group(Grouped(g2_), b2), rbase)) =>            
            val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

            val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
            val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

            val g = g1 ++ g2

            val fixup = Project.EmptyDoc.setAll(to.mapValues(f => -\/ (DocVar.ROOT(f))))

            val b = \/- (Reshape.Arr(ListMap(BsonField.Index(0) -> b1, BsonField.Index(1) -> b2)))

            consumeBoth(lbase, rbase)(Group(Grouped(g), b) :: fixup :: Nil)

          case Both((left @ Group(Grouped(g1_), b1), lbase), _) => 
            val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))
            val uniqVar = DocVar.ROOT(uniqName)

            val tmpG: PipelineOp = Group(Grouped(g1_ + (uniqName -> ExprOp.Push(rbase))), b1)

            \/- ((lbase, uniqVar) -> ConsumeLeft(tmpG :: Unwind(uniqVar) :: Nil))

          case Both(_, (Group(_, _), _)) => delegate

          case Both((left @ Project(_), lbase), (right @ Project(_), rbase)) => 
            consumeBoth(LeftVar \\ lbase, RightVar \\ rbase) {
              Project(Reshape.Doc(ListMap(LeftName -> \/- (left.shape), RightName -> \/- (right.shape)))) :: Nil
            }

          case Both((left @ Project(_), lbase), _) =>
            consumeBoth(LeftVar \\ lbase, RightVar \\ rbase) {
              Project(Reshape.Doc(ListMap(LeftName -> \/- (left.shape), RightName -> -\/ (DocVar.ROOT())))) :: Nil
            }

          case Both(_, (Project(_), _)) => delegate

          case Both((left @ Redact(_), lbase), (right @ Redact(_), rbase)) => 
            consumeBoth(lbase, rbase)(left :: right :: Nil)

          case Both((left @ Unwind(_), lbase), (right @ Unwind(_), rbase)) if left == right =>
            consumeBoth(lbase, rbase)(left :: Nil)

          case Both((left @ Unwind(_), lbase), (right @ Unwind(_), rbase)) =>
            consumeBoth(lbase, rbase)(left :: right :: Nil)

          case Both((left @ Unwind(_), lbase), (right @ Redact(_), rbase)) =>
            consumeRight(lbase, rbase)(right)

          case Both((Redact(_), _), (Unwind(_), _)) => delegate
        }
    }

    cogroup.statefulE(this.simplify.buffer.reverse, that.simplify.buffer.reverse)((DocVar.ROOT(), DocVar.ROOT()))(step).flatMap {
      case ((lbase, rbase), list) => f(lbase \\ this.base, rbase \\ that.base, list.reverse)
    }  
  }

  private def mergeGroups(groupBys0: List[PipelineBuilder]*): Error \/ List[PipelineBuilder] = {
    if (groupBys0.isEmpty) \/- (Nil)
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
      val One = PipelineBuilder.fromExpr(ExprOp.Literal(Bson.Int64(1L)))

      val maxLen = groupBys0.view.map(_.length).max

      val groupBys: List[List[PipelineBuilder]] = groupBys0.toList.map(_.reverse.padTo(maxLen, One).reverse)

      type EitherError[X] = Error \/ X

      groupBys.transpose.map {
        case Nil => \/- (One)
        case x :: xs => xs.foldLeftM[EitherError, PipelineBuilder](x) { (a, b) => 
          if (a == b) \/- (a) else for {
            a <- a.makeArray
            b <- b.makeArray
            c <- a.arrayConcat(b)
          } yield c
        }
      }.sequenceU
    }
  }
}
object PipelineBuilder {
  import PipelineOp._
  import ExprOp.{DocVar}

  private val ExprName  = BsonField.Name("expr")
  private val ExprVar   = ExprOp.DocVar.ROOT(ExprName)
  private val LeftName  = BsonField.Name("lEft")
  private val RightName = BsonField.Name("rIght")

  private val LeftVar   = DocVar.ROOT(LeftName)
  private val RightVar  = DocVar.ROOT(RightName)

  def empty = PipelineBuilder(Nil, DocVar.ROOT(), SchemaChange.Init, Nil)

  def fromExpr(expr: ExprOp): PipelineBuilder = {
    PipelineBuilder(
      buffer  = Project(Reshape.Doc(ListMap(ExprName -> -\/ (expr)))) :: Nil,
      base    = ExprVar,
      struct  = SchemaChange.Init,
      groupBy = Nil
    )
  }  

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
