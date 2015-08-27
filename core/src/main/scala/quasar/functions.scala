/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.recursionschemes._

import scalaz._

sealed trait Func {
  def name: String

  def help: String

  def codomain: Type

  def domain: List[Type]

  def simplify: Func.Simplifier

  def apply(args: Fix[LogicalPlan]*): Fix[LogicalPlan] = LogicalPlan.Invoke(this, args.toList)

  // TODO: Make this `unapplySeq`
  def unapply[A](node: LogicalPlan[A]): Option[List[A]] = {
    node match {
      case LogicalPlan.InvokeF(f, a) if f == this => Some(a)
      case _                                      => None
    }
  }

  def apply: Func.Typer

  val untype0: Func.Untyper

  def untype(tpe: Type) = untype0(this, tpe)

  final def apply(arg1: Type, rest: Type*): ValidationNel[SemanticError, Type] = apply(arg1 :: rest.toList)

  def mappingType: MappingType

  final def arity: Int = domain.length

  override def toString: String = name
}
trait FuncInstances {
  implicit val FuncRenderTree = new RenderTree[Func] {
    def render(v: Func) = Terminal(v.mappingType.toString :: "Func" :: Nil, Some(v.name))
  }
}
object Func extends FuncInstances {
  type Simplifier = List[Fix[LogicalPlan]] => Option[Fix[LogicalPlan]]
  type Typer      = List[Type] => ValidationNel[SemanticError, Type]
  type Untyper    = (Func, Type) => ValidationNel[SemanticError, List[Type]]
}

final case class Reduction(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToOne
}

final case class Expansion(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToMany
}

final case class ExpansionFlat(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToManyFlat
}

final case class Mapping(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.OneToOne
}

final case class Squashing(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.Squashing
}

final case class Sifting(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToMany
}

final case class Transformation(name: String, help: String, codomain: Type, domain: List[Type], simplify: Func.Simplifier, apply: Func.Typer, untype0: Func.Untyper) extends Func {
  def mappingType = MappingType.ManyToManyTransform
}

sealed trait MappingType

object MappingType {
  final case object OneToOne            extends MappingType
  final case object OneToMany           extends MappingType
  final case object OneToManyFlat       extends MappingType
  final case object ManyToOne           extends MappingType
  final case object ManyToMany          extends MappingType
  final case object ManyToManyTransform extends MappingType
  final case object Squashing           extends MappingType
}
