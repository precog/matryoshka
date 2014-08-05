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
  case object NotGrouped extends PipelineBuilderError {
    def message = "The pipeline builder has not been grouped by another set, so a group op doesn't make sense"
  }
}

/**
 * A `PipelineBuilder` consists of a list of pipeline operations in *reverse*
 * order, a structure, and a base mod for that structure.
 */
final case class PipelineBuilder private (buffer: List[PipelineOp], base: ExprOp.DocVar, struct: SchemaChange, groupBy: List[PipelineBuilder] = Nil) { self =>
  import PipelineBuilder._
  import PipelineOp._
  import ExprOp.{DocVar, GroupOp}

  def build: Pipeline = Pipeline(simplify.buffer.reverse) // FIXME when base schema is not Init

  def simplify: PipelineBuilder = copy(buffer = Project.simplify(buffer.reverse).reverse)

  private def asExprOp = this.simplify match {
    case PipelineBuilder(Project(Reshape.Doc(fields)) :: _, `ExprVar`, _, _) => 
      fields.toList match {
        case (`ExprName`, -\/ (e)) :: Nil => Some(e)
        case _ => None
      }

    case _ => None
  }

  private def isRootExpr = asExprOp.exists {
    case DocVar.ROOT() => true
    case _ => false
  }  

  def isExpr = asExprOp.isDefined

  def isGroupOp = asExprOp.map {
    case x : GroupOp => true
    case _ => false
  }

  def asLiteral = asExprOp.collect {
    case (x @ ExprOp.Literal(_)) => x
  }

  def map(f: DocVar => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
    for {
      that <- f(base)
    } yield copy(
        buffer = that.buffer ::: this.buffer,
        base   = that.base,
        struct = that.struct
      )
  }

  def unify(that: PipelineBuilder)(f: (DocVar, DocVar) => Error \/ PipelineBuilder): Error \/ PipelineBuilder = {
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
            val right = Project(Reshape.Doc(Map(RightName -> -\/ (rbase))))

            step((lbase, DocVar.ROOT()), Both(left, right)).map {
              case ((lbase, rbase), instr) => ((lbase, rbase \ RightName), instr)
            }

          case That(_) => delegate

          case Both((left : GeoNear, lbase), _) =>
            consumeLeft(lbase, rbase)(left)

          case Both(_, (_ : GeoNear, _)) => delegate

          case Both((left : ShapePreservingOp, lbase), _) => 
            consumeLeft(lbase, rbase)(left)

          case Both(_, right : ShapePreservingOp) => delegate

          case Both((Group(Grouped(g1_), b1), lbase), (Group(Grouped(g2_), b2), rbase)) if (b1 == b2) =>            
            val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

            val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
            val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

            val g = g1 ++ g2

            val fixup = Project(Reshape.Doc(Map())).setAll(to.mapValues(f => -\/ (DocVar.ROOT(f))))

            consumeBoth(lbase, rbase)(Group(Grouped(g), b1) :: fixup :: Nil)

          case Both((left @ Group(Grouped(g1_), b1), lbase), _) => 
            val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))
            val uniqVar = DocVar.ROOT(uniqName)

            val tmpG = Group(Grouped(Map(uniqName -> ExprOp.Push(DocVar.ROOT()))), b1)

            for {
              t <- step((lbase, rbase), Both(left, tmpG))

              ((lbase, rbase), ConsumeBoth(emit)) = t
            } yield ((lbase, uniqVar \\ rbase), ConsumeLeft(emit :+ Unwind(DocVar.ROOT(uniqName))))

          case Both(_, (Group(_, _), _)) => delegate

          case Both((left @ Project(_), lbase), (right @ Project(_), rbase)) => 
            consumeBoth(LeftVar \\ lbase, RightVar \\ rbase) {
              Project(Reshape.Doc(Map(LeftName -> \/- (left.shape), RightName -> \/- (right.shape)))) :: Nil
            }

          case Both((left @ Project(_), lbase), _) =>
            consumeBoth(LeftVar \\ lbase, RightVar \\ rbase) {
              Project(Reshape.Doc(Map(LeftName -> \/- (left.shape), RightName -> -\/ (DocVar.ROOT())))) :: Nil
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

    cogroup.statefulE(this.buffer.reverse, that.buffer.reverse)((DocVar.ROOT(), DocVar.ROOT()))(step).flatMap {
      case ((lbase, rbase), list) =>
        f(lbase \\ this.base, rbase \\ that.base).map { next =>
          if (next.isRootExpr) copy(buffer = list.reverse)
          else copy(
            buffer = next.buffer.reverse ::: list.reverse,
            base   = next.base,
            struct = next.struct
          )
        }
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
              buffer  = Project(Reshape.Doc(Map(ExprName -> -\/ (inner)))) :: buffer.tail,
              groupBy = bs
            )

            rewritten.unify(b) { (grouped, by) =>
              \/- (new PipelineBuilder(
                buffer = Group(Grouped(Map(BsonField.Name(name) -> construct(grouped))), -\/ (by)) :: Nil,
                base   = DocVar.ROOT(),
                struct = SchemaChange.Init.makeObject(name)
              ))
            }
        }
    }.getOrElse {
      \/- {
        copy(
          buffer  = Project(Reshape.Doc(Map(BsonField.Name(name) -> -\/ (base)))) :: buffer, 
          base    = DocVar.ROOT(),
          struct  = struct.makeObject(name)
        )
      }
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

  def projectField(name: String): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(Map(ExprName -> -\/ (base \ BsonField.Name(name))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectField(name)
      )
    }

  def projectIndex(index: Int): Error \/ PipelineBuilder = 
    \/- {
      copy(
        buffer  = Project(Reshape.Doc(Map(ExprName -> -\/ (base \ BsonField.Index(index))))) :: buffer, 
        base    = ExprVar, 
        struct  = struct.projectIndex(index)
      )
    }

  def groupBy(that: PipelineBuilder): Error \/ PipelineBuilder = {
    \/- (copy(groupBy = that :: groupBy))
  }

  def reduce(f: ExprOp => GroupOp): Error \/ PipelineBuilder = map(e => \/- (PipelineBuilder.fromExpr(f(e))))

  def isGrouped = !groupBy.isEmpty

  def sortBy(that: PipelineBuilder): Error \/ PipelineBuilder = {
    println(that.struct)
    ???
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

  def fromExprs(exprs: (String, ExprOp)*): PipelineBuilder = {
    PipelineBuilder(
      buffer = Project(Reshape.Doc(exprs.map(t => BsonField.Name(t._1) -> -\/ (t._2)).toMap)) :: Nil,
      base   = DocVar.ROOT(),
      struct = SchemaChange.Init
    )
  }    

  implicit def PipelineBuilderRenderTree(implicit RO: RenderTree[PipelineOp]) = new RenderTree[PipelineBuilder] {
    override def render(v: PipelineBuilder) = NonTerminal("PipelineBuilder", v.buffer.reverse.map(RO.render(_)))
  }
}
