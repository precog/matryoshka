package slamdata.engine

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def domain: List[Type]

  def apply: Func.Typer

  val unapply: Func.Untyper

  final def apply(arg1: Type, rest: Type*): ValidationNel[SemanticError, Type] = apply(arg1 :: rest.toList)  

  def mappingType: MappingType  

  final def arity: Int = domain.length
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

final case class Mapping(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToOne
}

final case class Transformation(name: String, help: String, domain: List[Type], apply: Func.Typer, unapply: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToMany
}

sealed trait MappingType 

object MappingType {
  case object OneToOne extends MappingType
  case object OneToMany extends MappingType
  case object ManyToOne extends MappingType
  case object ManyToMany extends MappingType
}