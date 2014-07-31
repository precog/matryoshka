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
final case class PipelineBuilder private (buffer: List[PipelineOp], base: ExprOp.DocVar, struct: SchemaChange) { self =>
  import PipelineBuilder._
  import PipelineOp._
  import ExprOp.{DocVar, GroupOp}

  def build: Pipeline = Pipeline(simplify.buffer.reverse) // FIXME when base schema is not Init

  def simplify: PipelineBuilder = copy(buffer = Project.simplify(buffer.reverse).reverse)

  def asLiteral = this.simplify match {
    case PipelineBuilder(Project(Reshape.Doc(fields)) :: Nil, `ExprVar`, _) => 
      fields.toList match {
        case (`ExprName`, -\/ (x @ ExprOp.Literal(_))) :: Nil => Some(x)
        case _ => None
      }

    case _ => None
  }

  def map(f: ExprOp => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
    for {
      that <- f(base)
    } yield PipelineBuilder(
        buffer = that.buffer ::: this.buffer,
        base   = that.base,
        struct = that.struct
      )
  }

  def unify(that: PipelineBuilder)(f: (DocVar, DocVar) => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
    import \&/._
    import cogroup.{Instr, ConsumeLeft, ConsumeRight, ConsumeBoth}

    def optRight[A, B](o: Option[A \/ B])(f: Option[A] => Error): Error \/ B = {
      o.map(_.fold(a => -\/ (f(Some(a))), b => \/- (b))).getOrElse(-\/ (f(None)))
    }

    type Out = Error \/ ((DocVar, DocVar), Instr[PipelineOp])

    lazy val step: ((DocVar, DocVar), PipelineOp \&/ PipelineOp) => Out = {
      case ((lbase, rbase), these) =>
        def delegate = step((rbase, lbase), these.swap).map {
          case ((rbase, lbase), instr) => ((lbase, rbase), instr.flip)
        }

        val these2 = these.fold[(PipelineOp, DocVar) \&/ (PipelineOp, DocVar)](
          left => This(rewrite(left, lbase)),
          right => That(rewrite(right, rbase)),
          (left, right) => Both(rewrite(left, lbase), rewrite(right, rbase))
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
          case This((left, lbase2)) => 
            val right = Project(Reshape.Doc(Map(RightName -> -\/ (DocVar.ROOT()))))

            val rbase2 = RightVar \\ rbase

            step((lbase2, rbase2), Both(left, right))
          
          case That((right, rbase2)) => 
            val left = Project(Reshape.Doc(Map(LeftName -> -\/ (DocVar.ROOT()))))

            val lbase2 = LeftVar \\ lbase

            step((lbase2, rbase2), Both(left, right))

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
            val uniqVar = DocVar.ROOT(uniqName)

            val tmpG = Group(Grouped(Map(uniqName -> ExprOp.Push(DocVar.ROOT()))), b1)

            for {
              t <- step((lbase, rbase), Both(left, tmpG))

              ((lbase, rbase), ConsumeBoth(emit)) = t
            } yield ((lbase, uniqVar \\ rbase), ConsumeLeft(emit :+ Unwind(DocVar.ROOT(uniqName))))

          case Both(_, (Group(_, _), _)) => delegate

          case Both((left @ Project(_), lbase2), (right @ Project(_), rbase2)) => 
            consumeBoth(LeftVar \\ lbase2, RightVar \\ rbase2) {
              Project(Reshape.Doc(Map(LeftName -> \/- (left.shape), RightName -> \/- (right.shape)))) :: Nil
            }

          case Both((left @ Project(_), lbase2), _) => 
            val uniqName: BsonField.Leaf = left.shape match {
              case Reshape.Arr(m) => BsonField.genUniqIndex(m.keys)
              case Reshape.Doc(m) => BsonField.genUniqName(m.keys)
            }

            consumeBoth(LeftVar \\ lbase2, RightVar \\ rbase) {
              Project(Reshape.Doc(Map(LeftName -> \/- (left.shape), RightName -> -\/ (DocVar.ROOT())))) :: Nil
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

    cogroup.statefulE(this.buffer.reverse, that.buffer.reverse)((DocVar.ROOT(), DocVar.ROOT()))(step).flatMap {
      case ((lbase, rbase), list) =>
        val lbase2 = lbase \\ this.base
        val rbase2 = rbase \\ that.base

        f(lbase2, rbase2).map { next =>
          PipelineBuilder(
            buffer = next.buffer ::: list.reverse,
            base   = next.base,
            struct = next.struct // FIXME: The structure could come from left or right or none, we need to know which!!!
          )
        }
    }
  }

  def makeObject(name: String): Error \/ PipelineBuilder = {
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(Map(BsonField.Name(name) -> -\/ (base)))) :: buffer, 
        base    = DocVar.ROOT(),
        struct  = struct.makeObject(name)
      )
    }
  }

  def makeArray: Error \/ PipelineBuilder = {
    \/- {
      copy(
        buffer  = Project(Reshape.Arr(Map(BsonField.Index(0) -> -\/ (base)))) :: buffer, 
        base    = DocVar.ROOT(),
        struct  = struct.makeArray(0)
      )
    }
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
                        base   = DocVar.ROOT(),
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
                        base   = DocVar.ROOT(),
                        struct = SchemaChange.MakeArray(m1 ++ m2)
                      )
                    }
                  }
        } yield rez

      // TODO: Here's where we'd handle Init case

      case _ => -\/ (PipelineBuilderError.CannotObjectConcatExpr)
    }
  }

  private def rewrite(op: PipelineOp, base: DocVar): (PipelineOp, DocVar) = {
    (op.rewriteRefs {
      case child => base \\ child
    }) -> (op match { case _ : ShapePreservingOp => base; case _ => DocVar.ROOT() })
  }

  def projectField(name: String): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(Map(ExprName -> -\/ (DocVar.ROOT(BsonField.Name(name)))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectField(name)
      )
    }

  def projectIndex(index: Int): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(Map(ExprName -> -\/ (DocVar.ROOT(BsonField.Index(index)))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectIndex(index)
      )
    }

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

  private val ExprName  = BsonField.Name("expr")
  private val ExprVar   = ExprOp.DocVar.ROOT(ExprName)
  private val LeftName  = BsonField.Name("left")
  private val RightName = BsonField.Name("right")

  private val LeftVar   = DocVar.ROOT(LeftName)
  private val RightVar  = DocVar.ROOT(RightName)

  def empty = PipelineBuilder(Nil, DocVar.ROOT(), SchemaChange.Init)

  def fromInit(op: ShapePreservingOp): PipelineBuilder = 
    PipelineBuilder(buffer = op :: Nil, base = DocVar.ROOT(), struct = SchemaChange.Init)

  def fromExpr(expr: ExprOp): PipelineBuilder = {
    PipelineBuilder(
      buffer = Project(Reshape.Doc(Map(ExprName -> -\/ (expr)))) :: Nil,
      base   = ExprVar,
      struct = SchemaChange.Init
    )
  }

  def apply(op: PipelineOp): PipelineBuilder = PipelineBuilder(
    buffer = op :: Nil,
    base   = DocVar.ROOT(),
    struct = SchemaChange.Init
  )

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
