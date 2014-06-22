package slamdata.engine

import slamdata.engine.analysis.fixplate._

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def domain: List[Type]

  def apply(args: Term[LogicalPlan]*): Term[LogicalPlan] = LogicalPlan.invoke(this, args.toList)

  def apply[A](args: List[A]): LogicalPlan[A] = LogicalPlan.Invoke(this, args)

  def unapply[A](node: LogicalPlan[A]): Option[List[A]] = {
    node.fold(
      read      = _ => None,
      constant  = _ => None,
      join      = (_, _, _, _, _, _) => None,
      invoke    = (f, a) => if (f == this) Some(a) else None,
      free      = _ => None,
      let       = (_, _) => None
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
  implicit def ShowFunc = new Show[Func] {
    override def show(v: Func) = Cord(v.name)
  }
}
object Func extends FuncInstances {
  type Typer   = List[Type] => ValidationNel[SemanticError, Type]
  type Untyper = Type => ValidationNel[SemanticError, List[Type]]
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