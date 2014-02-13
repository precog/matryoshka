package slamdata.engine

import scalaz._

import scalaz.std.vector._
import scalaz.std.indexedSeq._

import SemanticError.TypeError

sealed trait Type {
  def & (that: Type) = Type.Product(this, that)

  def | (that: Type) = Type.Coproduct(this, that)

  def unify(that: Type): ValidationNel[TypeError, Type]
}

trait TypeInstances {  
  val TypeOrMonoid = new Monoid[Type] {
    def zero = Type.Top

    def append(v1: Type, v2: => Type) = (v1, v2) match {
      case (Type.Top, that) => that
      case (this0, Type.Top) => this0
      case _ => v1 | v2
    }
  }

  val TypeAndMonoid = new Monoid[Type] {
    def zero = Type.Top

    def append(v1: Type, v2: => Type) = (v1, v2) match {
      case (Type.Top, that) => that
      case (this0, Type.Top) => this0
      case _ => v1 & v2
    }
  }
}

case object Type extends TypeInstances {
  private def fail(expected: Type, actual: Type, message: Option[String]): ValidationNel[TypeError, Type] = 
    Validation.failure(NonEmptyList(TypeError(expected, actual, message)))

  private def fail(expected: Type, actual: Type): ValidationNel[TypeError, Type] = fail(expected, actual, None)

  private def fail(expected: Type, actual: Type, msg: String): ValidationNel[TypeError, Type] = fail(expected, actual, Some(msg))

  case object Top extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = Validation.success(that)
  }

  case object Bottom extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Bottom => Validation.success(this)
      case _ => fail(this, that)
    }
  }

  case object Null extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Null => Validation.success(that)
      case _ => fail(this, that)
    }
  }

  sealed trait PrimitiveType extends Type

  case object Str extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Str => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to a string")
    }
  }

  case object Int extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Int => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to an integer")
    }
  }
  case object Dec extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Dec => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to a decimal")
    }
  }
  case object Bool extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Bool => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to a boolean")
    }
  }

  case object DateTime extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case DateTime => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to a date time")
    }
  }
  case object Interval extends PrimitiveType {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case Interval => Validation.success(that)
      case _ => fail(this, that, "Use CONVERT to convert the expression to an interval")
    }
  }

  case class HomSet(value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case HomSet(value2) => value.unify(value2)
      case _ => fail(this, that)
    }
  }

  private def unifyWithProduct(expected: Type, product: Product): ValidationNel[TypeError, Type] = {
    unifyDucts(expected :: Nil, product.flatten, exists _)(Type.TypeAndMonoid)
  }

  private def unifyWithCoproduct(expected: Type, coproduct: Coproduct): ValidationNel[TypeError, Type] = {
    unifyDucts(expected :: Nil, coproduct.flatten, forall _)(Type.TypeOrMonoid)
  }

  def makeArray(values: Seq[Type]): Type = {
    Product(values.zipWithIndex.map(t => ArrayElem(t._2, t._1)))
  }

  case class HomArray(value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case HomArray(value2) => value.unify(value2)
      case ArrayElem(index, value2) => value.unify(value2)
      case x : Product    => unifyWithProduct(this, x)
      case x : Coproduct  => unifyWithCoproduct(this, x)
      case _ => fail(this, that)
    }
  }

  case class ArrayElem(index: Int, value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case HomArray(value2) => value.unify(value2)
      case ArrayElem(index2, value2) if (index == index2) => value.unify(value2)
      case x : Product    => unifyWithProduct(this, x)
      case x : Coproduct  => unifyWithCoproduct(this, x)
      case _ => fail(this, that)
    }
  }

  case class HomObject(value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case HomObject(value2) => value.unify(value2)
      case ObjField(name, value2) => value.unify(value2)
      case x : Product => unifyWithProduct(this, x)
      case x : Coproduct => unifyWithCoproduct(this, x)
      case _ => fail(this, that)
    }
  }

  case class ObjField(name: String, value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case HomObject(value2) => value.unify(value2)
      case ObjField(name2, value2) if (name == name2) => value.unify(value2)
      case x : Product => unifyWithProduct(this, x)
      case x : Coproduct => unifyWithCoproduct(this, x)
      case _ => fail(this, that)
    }
  }

  case class IsOnly(value: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case _ => fail(this, that)
    }
  }

  def exists(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Type] = actuals.headOption match {
    case Some(head) => expected.unify(head) ||| exists(expected, actuals.tail)
    case None => fail(expected, Product(actuals))
  }

  def forall(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Type] = {
    implicit val m: Monoid[Type] = Type.TypeOrMonoid

    actuals.headOption match {
      case Some(head) => expected.unify(head) +++ exists(expected, actuals.tail)
      case None => Validation.success(Top)
    }
  }

  def unifyDucts(expecteds: Seq[Type], actuals: Seq[Type], 
            check: (Type, Seq[Type]) => ValidationNel[TypeError, Type])(implicit m: Monoid[Type]) = {
    expecteds.foldLeft[ValidationNel[TypeError, Type]](Validation.success(Top)) {
      case (acc, expected) => {
        acc +++ check(expected, actuals)
      }
    }
  }

  case class Product(left: Type, right: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case x : Product =>
        unifyDucts(flatten, x.flatten, exists _)(Type.TypeAndMonoid)

      case x : Coproduct =>
        unifyDucts(flatten, x.flatten, forall _)(Type.TypeOrMonoid)

      case _ => fail(this, that)
    }

    def flatten: Vector[Type] = {
      def flatten0(v: Type): Vector[Type] = v match {
        case Product(left, right) => flatten0(left) ++ flatten0(right)
        case x => Vector(x)
      }

      flatten0(this)
    }
  }
  object Product extends ((Type, Type) => Type) {
    def apply(values: Seq[Type]): Type = values.foldLeft[Type](Top)(_ & _)
  }

  case class Coproduct(left: Type, right: Type) extends Type {
    def unify(that: Type): ValidationNel[TypeError, Type] = that match {
      case x : Product =>
        unifyDucts(flatten, x.flatten, forall _)(Type.TypeOrMonoid)

      case x : Coproduct =>
        unifyDucts(flatten, x.flatten, exists _)(Type.TypeOrMonoid)

      case _ => fail(this, that)
    }

    def flatten: Vector[Type] = {
      def flatten0(v: Type): Vector[Type] = v match {
        case Coproduct(left, right) => flatten0(left) ++ flatten0(right)
        case x => Vector(x)
      }

      flatten0(this)
    }
  }
  object Coproduct extends ((Type, Type) => Type) {
    def apply(values: Seq[Type]): Type = values.foldLeft[Type](Top)(_ | _)
  }
}