package slamdata.engine

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def domain: List[Type]

  def codomain: Func.CodomainRefiner

  def mappingType: MappingType  

  final def arity: Int = domain.length
}
object Func {
  type CodomainRefiner = List[Type] => ValidationNel[SemanticError, Type]
}

final case class Reduction(name: String, help: String, domain: List[Type], codomain: Func.CodomainRefiner) extends Func {
  def mappingType = MappingType.ManyToOne
}

final case class Expansion(name: String, help: String, domain: List[Type], codomain: Func.CodomainRefiner) extends Func {
  def mappingType = MappingType.OneToMany
}

final case class Mapping(name: String, help: String, domain: List[Type], codomain: Func.CodomainRefiner) extends Func {
  def mappingType = MappingType.OneToOne
}

final case class Transformation(name: String, help: String, domain: List[Type], codomain: Func.CodomainRefiner) extends Func {
  def mappingType = MappingType.ManyToMany
}

sealed trait MappingType 

object MappingType {
  case object OneToOne extends MappingType
  case object OneToMany extends MappingType
  case object ManyToOne extends MappingType
  case object ManyToMany extends MappingType
}