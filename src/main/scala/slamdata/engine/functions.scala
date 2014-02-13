package slamdata.engine

case class Func(name: String, help: String, domain: Seq[Type], codomain: Type, mappingType: MappingType, codomainSpecific: Option[Seq[Type] => Type] = None) {

  def codomainFor(args: Seq[Type]): Type = codomainSpecific.map(f => f(args)).getOrElse(codomain)

  final def arity: Int = domain.length
}

sealed trait MappingType 

object MappingType {
  case object OneToOne extends MappingType
  case object OneToMany extends MappingType
  case object ManyToOne extends MappingType
}