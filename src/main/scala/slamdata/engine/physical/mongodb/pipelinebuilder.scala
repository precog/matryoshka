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

  def build: Pipeline = Pipeline(buffer.reverse) // FIXME

  def asLiteral = {
    def asExprOp[A <: ExprOp](pf: PartialFunction[ExprOp, A]): Error \/ A = {
      def extract(o: Option[ExprOp \/ Reshape]): Error \/ A = {
        o.map(_.fold(
          e => pf.lift(e).map(\/- apply).getOrElse(-\/ (PipelineBuilderError.NotExpectedExpr)), 
          _ => -\/ (PipelineBuilderError.NotExpr)
        )).getOrElse(-\/ (PipelineBuilderError.NotExpr))
      }

      for {
        rootRef <-  rootRef.map(_.field)
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

    lazy val step: ((SchemaChange, SchemaChange), PipelineOp \&/ PipelineOp) => 
           Error \/ ((SchemaChange, SchemaChange), Instr[PipelineOp]) = {
      case ((lschema, rschema), these) =>
        def delegate = step((rschema, lschema), these.swap).map {
          case ((lschema, rschema), instr) => ((rschema, lschema), instr.flip)
        }

        these match {
          case This(left) => 
            val rchange = SchemaChange.Init.makeObject("right")

            for {
              rproj <- optRight(rchange.toProject)(_ => PipelineBuilderError.UnexpectedExpr(rchange))

              rschema2 = rschema.rebase(rchange)

              rec <- step((lschema, rschema2), Both(left, rproj))
            } yield rec
          
          case That(right) => 
            val lchange = SchemaChange.Init.makeObject("left")

            for {
              lproj <- optRight(lchange.toProject)(_ => PipelineBuilderError.UnexpectedExpr(lchange))

              lschema2 = rschema.rebase(lchange)

              rec <- step((lschema2, rschema), Both(lproj, right))
            } yield rec

          case Both(g1 : GeoNear, g2 : GeoNear) if g1 == g2 => 
            \/- ((lschema, rschema) -> ConsumeBoth(g1 :: Nil))

          case Both(g1 : GeoNear, right) =>
            \/- ((lschema, rschema) -> ConsumeLeft(g1 :: Nil))

          case Both(left, g2 : GeoNear) => delegate

          case Both(left : ShapePreservingOp, _) => 
            \/- ((lschema, rschema) -> ConsumeLeft(left :: Nil))

          case Both(_, right : ShapePreservingOp) => 
            \/- ((lschema, rschema) -> ConsumeRight(right :: Nil))

          case Both(x @ Group(Grouped(g1_), b1), y @ Group(Grouped(g2_), b2)) if (b1 == b2) =>            
            val (to, _) = BsonField.flattenMapping(g1_.keys.toList ++ g2_.keys.toList)

            val g1 = g1_.map(t => (to(t._1): BsonField.Leaf) -> t._2)
            val g2 = g2_.map(t => (to(t._1): BsonField.Leaf) -> t._2)

            val g = g1 ++ g2

            val fixup = Project(Reshape.Doc(Map())).setAll(to.mapValues(f => -\/ (DocVar.ROOT(f))))

            \/- ((lschema, rschema) -> ConsumeBoth(Group(Grouped(g), b1) :: fixup :: Nil))

          case Both(x @ Group(Grouped(g1_), b1), _) => 
            val uniqName = BsonField.genUniqName(g1_.keys.map(_.toName))

            val tmpG = Group(Grouped(Map(uniqName -> ExprOp.Push(DocVar.ROOT()))), b1)

            for {
              t <- step((lschema, rschema), Both(x, tmpG))

              ((lschema, rschema), ConsumeBoth(emit)) = t
            } yield ((lschema, rschema.makeObject(uniqName.value)), ConsumeLeft(emit :+ Unwind(DocVar.ROOT(uniqName))))

          case Both(_, Group(_, _)) => delegate

          case Both(p1 @ Project(_), p2 @ Project(_)) => 
            val Left  = BsonField.Name("left")
            val Right = BsonField.Name("right")

            val LeftV  = DocVar.ROOT(Left)
            val RightV = DocVar.ROOT(Right)

            \/- {
              ((lschema.makeObject("left"), rschema.makeObject("right")) -> 
               ConsumeBoth(Project(Reshape.Doc(Map(Left -> \/- (p1.shape), Right -> \/- (p2.shape)))) :: Nil))
            }

          case Both(p1 @ Project(_), right) => 
            val uniqName: BsonField.Leaf = p1.shape match {
              case Reshape.Arr(m) => BsonField.genUniqIndex(m.keys)
              case Reshape.Doc(m) => BsonField.genUniqName(m.keys)
            }

            val Left  = BsonField.Name("left")
            val Right = BsonField.Name("right")

            val LeftV  = DocVar.ROOT(Left)
            val RightV = DocVar.ROOT(Right)

            \/- {
              ((lschema.makeObject("left"), rschema.makeObject("right")) ->
                ConsumeLeft(Project(Reshape.Doc(Map(Left -> \/- (p1.shape), Right -> -\/ (DocVar.ROOT())))) :: Nil))
            }

          case Both(_, Project(_)) => delegate

          case Both(r1 @ Redact(_), r2 @ Redact(_)) => 
            \/- ((lschema, rschema) -> ConsumeBoth(r1 :: r2 :: Nil))

          case Both(u1 @ Unwind(_), u2 @ Unwind(_)) if u1 == u2 =>
            \/- ((lschema, rschema) -> ConsumeBoth(u1 :: Nil))

          case Both(u1 @ Unwind(_), u2 @ Unwind(_)) =>
            \/- ((lschema, rschema) -> ConsumeBoth(u1 :: u2 :: Nil))

          case Both(u1 @ Unwind(_), r2 @ Redact(_)) =>
            \/- ((lschema, rschema) -> ConsumeRight(r2 :: Nil))

          case Both(r1 @ Redact(_), u2 @ Unwind(_)) => delegate
        }
    }

    cogroup.statefulE(this.buffer.reverse, that.buffer.reverse)((this.base, that.base))(step).flatMap {
      case ((lschema, rschema), list) =>
        val left  = lschema.patchRoot(SchemaChange.Init)
        val right = rschema.patchRoot(SchemaChange.Init)

        (left |@| right) { (left0, right0) =>
          val left  = left0.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))
          val right = right0.fold(_ => DocVar.ROOT(), DocVar.ROOT(_))

          f(left, right).map { that =>
            PipelineBuilder(
              buffer = that.buffer ::: self.buffer, 
              base   = that.base.rebase(self.base).simplify,
              struct = that.struct.rebase(self.struct).simplify
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

      case _ => -\/ (PipelineBuilderError.CannotObjectConcatExpr)
    }
  }

  def + (op: ShapePreservingOp): Error \/ PipelineBuilder = {
    for {
      rootRef <- rootRef
    } yield copy(buffer = op.rewriteRefs {
      case d : DocVar => rootRef \ d.field
    } :: buffer)
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
      base   = SchemaChange.Init.projectField(ExprName.value),
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
