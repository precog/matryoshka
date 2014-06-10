package slamdata.engine

import slamdata.engine.fs._
import slamdata.engine.analysis.fixplate._

import scalaz._
import Scalaz._

object MRA {
  // foo[*].bar + foo[*].baz
  // foo.bar[*] + foo.baz[*]
  // foo[*] + baz[*]
  // 


  import LogicalPlan._

  final case class Dims private (contracts: List[DimContract], id: Set[DimId], expands: List[DimExpand]) {
    def size = contracts.length + (if (value) 0 else 1) + expands.length

    def value = id.size == 0 || squash.id == Set(DimId.Value)

    def contract: Dims = copy(contracts = DimContract :: contracts)

    def flatten: Dims = withNormalized(n => n.copy(id = n.id.map(DimId.Flatten(_))))

    def expand: Dims = copy(expands = DimExpand :: expands)

    def contracted = contracts.length > 0

    def squash = copy(contracts = Nil, id = id.map(_.squash), expands = Nil).normalize

    def squashed = squash.id == DimId.simplify(id)

    def field(name: String): Dims = withNormalized(n => n.copy(id = n.id.map(DimId.ObjProj(_, name))))

    def index(idx: Long): Dims = withNormalized(n => n.copy(id = n.id.map(DimId.ArrProj(_, idx))))

    def identified = !value

    def expanded = expands.length > 0

    def ++ (that: Dims) = Dims(
      if (contracts.length > that.contracts.length) contracts else that.contracts,
      DimId.simplify(this.id ++ that.id),
      if (expands.length > that.expands.length) expands else that.expands
    )

    def aggregate: Dims = {
      if (expands.length > 0) copy(expands = expands.tail)
      else if (!id.isEmpty) copy(id = Set())
      else if (contracts.length > 0) copy(contracts = contracts.tail)
      else this
    }

    def normalize: Dims = if (id.size == 0) copy(id = Set(DimId.Value)) else copy(id = DimId.simplify(id))

    private def withNormalized(f: Dims => Dims) = {
      val n = normalize

      f(n)
    }
  }
  object Dims {
    def Value = id(Set[DimId](DimId.Value))

    def set(paths: Path*) = new Dims(Nil, Set(paths: _*).map(DimId.Source.apply), Nil)

    def combineAll(list: List[Dims]): Dims = list match {
      case Nil => Value
      case head :: tail => tail.foldLeft(head)(_ ++ _).normalize
    }

    def id(ids: Set[DimId]) = new Dims(Nil, ids, Nil)
  }

  sealed trait DimId {
    import DimId._

    def subsumes(that: DimId): Boolean = (this, that) match {
      case (x, y) if (x == y) => true

      case (ArrProj(o, _), _) => o.subsumes(that)
      case (ObjProj(o, _), _) => o.subsumes(that)
      case (Flatten(o), _)    => o.subsumes(that)

      case _ => false
    }

    def intersect(that: DimId): Option[DimId] = (this, that) match {
      case (x, y) if (x == y) => Some(x)

      case (ArrProj(o, _), _) => o.intersect(that) orElse (that.intersect(o))
      case (ObjProj(o, _), _) => o.intersect(that) orElse (that.intersect(o))
      case (Flatten(o), _)    => o.intersect(that) orElse (that.intersect(o))

      case _ => None
    }

    def squash: DimId = this match {
      case ArrProj(on, _) => on.squash
      case ObjProj(on, _) => on.squash
      case Flatten(on)    => on.squash

      case x => x
    }

    def id: String = this match {
      case Value => "<value>"
      case Source(path) => path.pathname
      case ArrProj(on, index) => on.id + ("[" + index + "]")
      case ObjProj(on, name)  => on.id + ("{" + name  + "}")
      case Flatten(on) => on.id + "(*)"
    }

    def index(idx: Long): DimId = ArrProj(this, idx)

    def field(name: String): DimId = ObjProj(this, name)

    def flatten: DimId = Flatten(this)

    override def toString = id
  }
  object DimId {
    final case object Value extends DimId
    final case class Source(path: Path) extends DimId
    final case class ArrProj(on: DimId, index: Long) extends DimId
    final case class ObjProj(on: DimId, name: String) extends DimId
    final case class Flatten(on: DimId) extends DimId

    def simplify(s: Set[DimId]): Set[DimId] = s.foldLeft(Set.empty[DimId]) {
      case (acc, d) => 
        val acc2 = acc.filter(d2 => !(d subsumes d2))

        acc2 ++ (if (acc2.exists(_ subsumes d)) Set() else Set(d))
    }
  }

  sealed trait DimContract
  case object DimContract extends DimContract
  
  sealed trait DimExpand
  case object DimExpand extends DimExpand

  def DimsPhase[A]: PhaseE[LogicalPlan, PlannerError, A, Dims] = lpBoundPhaseE {
    type Output = Dims
    
    liftPhaseE(Phase { (attr: LPAttr[A]) =>
      synthPara2(forget(attr)) { (node: LogicalPlan[(LPTerm, Output)]) =>
        node.fold[Output](
          read      = Dims.set(_), 
          constant  = Function.const(Dims.Value),
          join      = (left, right, tpe, rel, lproj, rproj) => ???,
          invoke    = (func, args) =>  {
                        val d = Dims.combineAll(args.map(_._2))
                        
                        import MappingType._ 

                        func.mappingType match {
                          case OneToOne   => d
                          case OneToMany  => d.expand
                          case OneToManyF => d.flatten
                          case ManyToOne  => d.aggregate
                          case ManyToMany => d
                          case Squashing  => d.squash
                        }
                      },
          free      = Function.const(Dims.Value),
          let       = (let, in) => in._2
        )
      }
    })
  }
}