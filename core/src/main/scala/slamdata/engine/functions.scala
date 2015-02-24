package slamdata.engine

import slamdata.engine.fp._
import slamdata.engine.analysis.fixplate._

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def domain: List[Type]

  def apply(args: Term[LogicalPlan]*): Term[LogicalPlan] = LogicalPlan.Invoke(this, args.toList)

  def unapply[A](node: LogicalPlan[A]): Option[List[A]] = {
    node.fold(
      read      = κ(None),
      constant  = κ(None),
      join      = κ(None),
      invoke    = (f, a) => if (f == this) Some(a) else None,
      free      = κ(None),
      let       = κ(None)
    )
  }

  def apply: Func.Typer

  val unapply: Func.Untyper

  final def apply(arg1: Type, rest: Type*): ValidationNel[SemanticError, Type] = apply(arg1 :: rest.toList)

  def mappingType: MappingType

  final def arity: Int = domain.length

  override def toString: String = name
}
trait FuncInstances {
  implicit val FuncRenderTree = new RenderTree[Func] {
    override def render(v: Func) = Terminal(v.name, List("Func", v.mappingType.toString))
  }
}
object Func extends FuncInstances {
  type Typer   = List[Type] => ValidationNel[SemanticError, Type]
  type Untyper = Type => ValidationNel[SemanticError, List[Type]]
}

trait VirtualFunc {
  def apply(args: Term[LogicalPlan]*): Term[LogicalPlan]

  def unapply(t: Term[LogicalPlan]): Option[List[Term[LogicalPlan]]] = Attr.unapply(attrK(t, ())).map(l => l.map(forget(_)))

  def Attr: VirtualFuncAttrExtractor
  trait VirtualFuncAttrExtractor {
    import slamdata.engine.analysis.fixplate.{Attr => FAttr}

    def unapply[A](t: FAttr[LogicalPlan, A]): Option[List[FAttr[LogicalPlan, A]]]
  }
}

final case class Reduction(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToOne
}

final case class Expansion(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToMany
}

final case class ExpansionFlat(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToManyFlat
}

final case class Mapping(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToOne
}

final case class Squashing(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.Squashing
}

final case class Transformation(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToMany
}

sealed trait MappingType

object MappingType {
  case object OneToOne      extends MappingType
  case object OneToMany     extends MappingType
  case object OneToManyFlat extends MappingType
  case object ManyToOne     extends MappingType
  case object ManyToMany    extends MappingType
  case object Squashing     extends MappingType
}
