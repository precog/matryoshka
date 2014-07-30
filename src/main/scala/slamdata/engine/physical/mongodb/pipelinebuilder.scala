package slamdata.engine.physical.mongodb

import scalaz._
import Scalaz._

import slamdata.engine.{RenderTree, Terminal, NonTerminal, Error}
import slamdata.engine.fp._

sealed trait PipelineBuilderError extends Error
object PipelineBuilderError {
  case class UnexpectedExpr(schema: SchemaChange) extends PipelineBuilderError {
    def message = "Unexpected expression: " + schema 
  }
  case object CouldNotPatchRoot extends PipelineBuilderError {
    def message = "Could not patch ROOT"
  }
  case object CannotObjectConcatExpr extends PipelineBuilderError {
    def message = "Cannot object concat an expression"
  }
  case object CannotArrayConcatExpr extends PipelineBuilderError {
    def message = "Cannot array concat an expression"
  }
  case object NotExpr extends PipelineBuilderError {
    def message = "The pipeline builder does not represent an expression"
  }
  case object NotExpectedExpr extends PipelineBuilderError {
    def message = "The expression does not have the expected shape"
  }
}

/**
 * A `PipelineBuilder` consists of a list of pipeline operations in *reverse*
 * order, a structure, and a base mod for that structure.
 */
final case class PipelineBuilder private (buffer: List[PipelineOp], base: SchemaChange, struct: SchemaChange) { self =>
  import PipelineBuilder._
  import PipelineOp._
  import ExprOp.{DocVar, GroupOp}

  def build: Pipeline = Pipeline(buffer.reverse) // FIXME when base schema is not Init

  def simplify: PipelineBuilder = copy(buffer = Project.simplify(buffer.reverse).reverse)

  def asLiteral = {
    def asExprOp[A <: ExprOp](pf: PartialFunction[ExprOp, A]): Error \/ A = {
      def extract(o: Option[ExprOp \/ Reshape]): Error \/ A = {
        o.map(_.fold(
          e => pf.lift(e).map(\/- apply).getOrElse(-\/ (PipelineBuilderError.NotExpectedExpr)), 
          _ => -\/ (PipelineBuilderError.NotExpr)
        )).getOrElse(-\/ (PipelineBuilderError.NotExpr))
      }

      for {
        rootRef <-  rootRef
        rez     <-  buffer.foldLeft[Option[Error \/ A]](None) {
                      case (None, p @ Project(_))   => Some(extract(p.get(rootRef)))

                      case (None, g @ Group(_, _))  => Some(extract(g.get(rootRef)))

                      case (acc, _) => acc
                    }.getOrElse(-\/ (PipelineBuilderError.NotExpr))
      } yield rez
    }

    asExprOp { case x : ExprOp.Literal => x }
  }

  def map(f: ExprOp => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
    for {
      rootRef <- rootRef
      that    <- f(rootRef)
    } yield PipelineBuilder(
        buffer = that.buffer ::: this.buffer,
        base   = that.base.rebase(this.base),
        struct = that.struct.rebase(this.struct)
      )
  }

  def unify(that: PipelineBuilder)(f: (DocVar, DocVar) => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
    import \&/._
    import cogroup.{Instr, ConsumeLeft, ConsumeRight, ConsumeBoth}

    def optRight[A, B](o: Option[A \/ B])(f: Option[A] => Error): Error \/ B = {
      o.map(_.fold(a => -\/ (f(Some(a))), b => \/- (b))).getOrElse(-\/ (f(None)))
    }

    type Out = Error \/ ((SchemaChange, SchemaChange), Instr[PipelineOp])

    lazy val step: ((SchemaChange, SchemaChange), PipelineOp \&/ PipelineOp) => Out = {
      case ((lbase, rbase), these) =>
        def delegate = step((rbase, lbase), these.swap).map {
          case ((rbase, lbase), instr) => ((lbase, rbase), instr.flip)
        }

        val these2 = these.fold[Error \/ ((PipelineOp, SchemaChange) \&/ (PipelineOp, SchemaChange))](
          rewrite(_, lbase).map(This.apply),

          rewrite(_, rbase).map(That.apply),

          (left, right) => for {
            left2  <- rewrite(left, lbase)
            right2 <- rewrite(right, rbase)
          } yield Both(left2, right2)
        )

        def consumeLeft(lbase: SchemaChange, rbase: SchemaChange)(op: PipelineOp) : Out = {
          \/- ((lbase, rbase) -> ConsumeLeft(op :: Nil))
        }

        def consumeRight(lbase: SchemaChange, rbase: SchemaChange)(op: PipelineOp) : Out = {
          \/- ((lbase, rbase) -> ConsumeRight(op :: Nil))
        }

        def consumeBoth(lbase: SchemaChange, rbase: SchemaChange)(ops: List[PipelineOp]) : Out = {
          \/- ((lbase, rbase) -> ConsumeBoth(ops))
        }

        these2.flatMap {
          case This((left, lbase2)) => 
            val rchange = SchemaChange.Init.makeObject("right")

            for {
              rproj <- optRight(rchange.toProject)(_ => PipelineBuilderError.UnexpectedExpr(rchange))

              rbase2 = rchange.rebase(rbase)

              rec <- step((lbase2, rbase2), Both(left, rproj))
            } yield rec
          
          case That((right, rbase2)) => 
            val lchange = SchemaChange.Init.makeObject("left")

            for {
              lproj <- optRight(lchange.toProject)(_ => PipelineBuilderError.UnexpectedExpr(lchange))

              lbase2 = lchange.rebase(lbase)

              rec <- step((lbase2, rbase2), Both(lproj, right))
            } yield rec

          case Both((g1 : GeoNear, lbase2), (g2 : GeoNear, rbase2)) if g1 == g2 => 
            consumeBoth(lbase2, rbase2)(g1 :: Nil)

          case Both((g1 : GeoNear, lbase2), _) =>
            consumeLeft(lbase2, rbase)(g1)

          case Both(_, (g2 : GeoNear, _)) => delegate

          case Both((left : ShapePreservingOp, lbase2), _) => 
            consumeLeft(lbase2, rbase)(left)

          case Both(_, right : ShapePreservingOp) => delegate

          case Both((Group(Grouped(g1_), b1), lbase2), (Group(Grouped(g2_), b2), rbase2)) if (b1 == b2) =>            
            val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

            val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
            val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

            val g = g1 ++ g2

            val fixup = Project(Reshape.Doc(Map())).setAll(to.mapValues(f => -\/ (DocVar.ROOT(f))))

            consumeBoth(lbase2, rbase2)(Group(Grouped(g), b1) :: fixup :: Nil)

          case Both((left @ Group(Grouped(g1_), b1), lbase2), _) => 
            val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))

            val tmpG = Group(Grouped(Map(uniqName -> ExprOp.Push(DocVar.ROOT()))), b1)

            for {
              t <- step((lbase, rbase), Both(left, tmpG))

              ((lbase, rbase), ConsumeBoth(emit)) = t
            } yield ((lbase, rbase.makeObject(uniqName.value)), ConsumeLeft(emit :+ Unwind(DocVar.ROOT(uniqName))))

          case Both(_, (Group(_, _), _)) => delegate

          case Both((left @ Project(_), lbase2), (right @ Project(_), rbase2)) => 
            val Left  = BsonField.Name("left")
            val Right = BsonField.Name("right")

            val LeftV  = DocVar.ROOT(Left)
            val RightV = DocVar.ROOT(Right)

            consumeBoth(lbase2.makeObject("left"), rbase2.makeObject("right")) {
              Project(Reshape.Doc(Map(Left -> \/- (left.shape), Right -> \/- (right.shape)))) :: Nil
            }

          case Both((left @ Project(_), lbase2), _) => 
            val uniqName: BsonField.Leaf = left.shape match {
              case Reshape.Arr(m) => BsonField.genUniqIndex(m.keys)
              case Reshape.Doc(m) => BsonField.genUniqName(m.keys)
            }

            val Left  = BsonField.Name("left")
            val Right = BsonField.Name("right")

            val LeftV  = DocVar.ROOT(Left)
            val RightV = DocVar.ROOT(Right)

            consumeBoth(lbase2.makeObject("left"), rbase.makeObject("right")) {
              Project(Reshape.Doc(Map(Left -> \/- (left.shape), Right -> -\/ (DocVar.ROOT())))) :: Nil
            }

          case Both(_, (Project(_), _)) => delegate

          case Both((left @ Redact(_), lbase2), (right @ Redact(_), rbase2)) => 
            consumeBoth(lbase, rbase)(left :: right :: Nil)

          case Both((left @ Unwind(_), lbase2), (right @ Unwind(_), rbase2)) if left == right =>
            consumeBoth(lbase2, rbase2)(left :: Nil)

          case Both((left @ Unwind(_), lbase2), (right @ Unwind(_), rbase2)) =>
            consumeBoth(lbase, rbase)(left :: right :: Nil)

          case Both((left @ Unwind(_), lbase2), (right @ Redact(_), rbase2)) =>
            consumeRight(lbase2, rbase)(right)

          case Both((Redact(_), _), (Unwind(_), _)) => delegate
        }
    }

    val Init = SchemaChange.Init : SchemaChange

    cogroup.statefulE(this.buffer.reverse, that.buffer.reverse)((Init, Init))(step).flatMap {
      case ((lbase, rbase), list) =>
        val left  = lbase.patchRoot(SchemaChange.Init)
        val right = rbase.patchRoot(SchemaChange.Init)

        (left |@| right) { (left0, right0) =>
          val left  = left0.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))
          val right = right0.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))

          f(left, right).map { next =>
            PipelineBuilder(
              buffer = next.buffer ::: list.reverse,
              base   = next.base,
              struct = next.struct // FIXME: The structure could come from left or right or none, we need to know which!!!
            )
          }
        }.getOrElse(-\/ (PipelineBuilderError.CouldNotPatchRoot))
    }
  }

  private def rootRef: Error \/ DocVar = {
    base.patchRoot(SchemaChange.Init).map(_.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))).map(\/- apply).getOrElse {
      -\/ (PipelineBuilderError.CouldNotPatchRoot)
    }
  }

  def makeObject(name: String): Error \/ PipelineBuilder = {
    for {
      rootRef <- rootRef
    } yield copy(
        buffer  = Project(Reshape.Doc(Map(BsonField.Name(name) -> -\/ (rootRef)))) :: buffer, 
        base    = SchemaChange.Init,
        struct  = struct.makeObject(name)
      )
  }

  def makeArray: Error \/ PipelineBuilder = {
    for {
      rootRef <- rootRef 
    } yield copy(
        buffer  = Project(Reshape.Arr(Map(BsonField.Index(0) -> -\/ (rootRef)))) :: buffer, 
        base    = SchemaChange.Init,
        struct  = struct.makeArray(0)
      )
  }

  def objectConcat(that: PipelineBuilder): Error \/ PipelineBuilder = {
    (this.struct.simplify, that.struct.simplify) match {
      case (s1 @ SchemaChange.MakeObject(m1), s2 @ SchemaChange.MakeObject(m2)) =>
        def convert(root: DocVar) = (keys: Iterable[String]) => 
          keys.map(BsonField.Name.apply).map(name => name -> -\/ (root \ name))

        for {
          rez <-  this.unify(that) { (left, right) =>
                    val leftTuples  = convert(left)(m1.keys)
                    val rightTuples = convert(right)(m2.keys)

                    \/- {
                      new PipelineBuilder(
                        buffer = Project(Reshape.Doc((leftTuples ++ rightTuples).toMap)) :: Nil,
                        base   = SchemaChange.Init,
                        struct = SchemaChange.MakeObject(m1 ++ m2)
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
        def convert(root: DocVar) = (keys: Iterable[Int]) => 
          keys.map(BsonField.Index.apply).map(index => index -> -\/ (root \ index))

        for {
          rez <-  this.unify(that) { (left, right) =>
                    val leftTuples  = convert(left)(m1.keys)
                    val rightTuples = convert(right)(m2.keys)

                    \/- {
                      new PipelineBuilder(
                        buffer = Project(Reshape.Arr((leftTuples ++ rightTuples).toMap)) :: Nil,
                        base   = SchemaChange.Init,
                        struct = SchemaChange.MakeArray(m1 ++ m2)
                      )
                    }
                  }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (PipelineBuilderError.CannotObjectConcatExpr)
    }
  }

  private def rewrite(op: PipelineOp, base: SchemaChange): Error \/ (PipelineOp, SchemaChange) = {
    for {
      rootRef <-  base.patchRoot(SchemaChange.Init).map(_.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))).map(\/- apply).getOrElse {
                    -\/ (PipelineBuilderError.CouldNotPatchRoot)
                  }
    } yield (op.rewriteRefs {
      case DocVar(_, Some(field)) => rootRef \ field
      case DocVar(_, None) => rootRef
    }) -> (op match { case _ : ShapePreservingOp => base; case _ => SchemaChange.Init })
  }

  def projectField(name: String): Error \/ PipelineBuilder = 
    \/- (copy(base = base.makeObject(name), struct = struct.projectField(name)))

  def projectIndex(index: Int): Error \/ PipelineBuilder = 
    \/- (copy(base = base.makeArray(index), struct = struct.projectIndex(index)))

  def groupBy(that: PipelineBuilder): Error \/ PipelineBuilder = {
    ???
  }

  def grouped(expr: GroupOp): Error \/ PipelineBuilder = {
    ???
  }

  def sortBy(that: PipelineBuilder): Error \/ PipelineBuilder = {
    ???
  }
}
object PipelineBuilder {
  import PipelineOp._
  import ExprOp.{DocVar}

  private val ExprName = BsonField.Name("expr")
  private val ExprVar  = ExprOp.DocVar.ROOT(ExprName)

  def empty = PipelineBuilder(Nil, SchemaChange.Init, SchemaChange.Init)

  def fromInit(op: ShapePreservingOp): PipelineBuilder = 
    PipelineBuilder(buffer = op :: Nil, base = SchemaChange.Init, struct = SchemaChange.Init)

  def fromExpr(expr: ExprOp): PipelineBuilder = {
    PipelineBuilder(
      buffer = Project(Reshape.Doc(Map(ExprName -> -\/ (expr)))) :: Nil,
      base   = SchemaChange.Init.makeObject(ExprName.value),
      struct = SchemaChange.Init
    )
  }

  def apply(op: PipelineOp): PipelineBuilder = PipelineBuilder(
    buffer = op :: Nil,
    base   = SchemaChange.Init,
    struct = SchemaChange.Init
  )

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
