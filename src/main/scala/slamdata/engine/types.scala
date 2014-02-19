package slamdata.engine

import scalaz._

import scalaz.std.vector._
import scalaz.std.indexedSeq._

import SemanticError.TypeError

sealed trait Type { self =>
  final def & (that: Type) = Type.Product(this, that)

  final def | (that: Type) = Type.Coproduct(this, that)

  def generalize = self
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

  private def succeed(tpe: Type): ValidationNel[TypeError, Type] = Validation.success(tpe)

  def typecheck(actual: Type, expected: Type): ValidationNel[TypeError, Type] = (actual, expected) match {
    case (Top, expected) => succeed(expected)

    case (actual, expected) if (actual == expected) => succeed(actual)

    case (Const(_), _) => fail(actual, expected) // they're not equal so we can't unify

    case (actual, Const(data)) => typecheck(actual, data.dataType) // try to generalize the const

    case (expected :Product, actual : Product) => unifyDucts(expected.flatten, actual.flatten, exists _)(Type.TypeAndMonoid)

    case (expected : Product, actual : Coproduct) => unifyDucts(expected.flatten, actual.flatten, forall _)(Type.TypeOrMonoid)

    case (expected : Coproduct, actual : Product) => unifyDucts(expected.flatten, actual.flatten, forall _)(Type.TypeOrMonoid)

    case (expected : Coproduct, actual : Coproduct) => unifyDucts(expected.flatten, actual.flatten, exists _)(Type.TypeOrMonoid)

    case (HomObject(expected), ObjField(name, actual)) => typecheck(expected, actual)

    case (ObjField(name, expected), HomObject(actual)) => typecheck(expected, actual) // Should be a warning

    case (ObjField(name1, expected), ObjField(name2, actual)) if (name1 == name2) => typecheck(expected, actual)

    case (HomArray(expected), ArrayElem(idx, actual)) => typecheck(expected, actual)

    case (ArrayElem(idx, expected), HomArray(actual)) => typecheck(expected, actual) // Should be a warning

    case (ArrayElem(idx1, expected), ArrayElem(idx2, actual)) if (idx1 == idx2) => typecheck(expected, actual)

    case _ => fail(actual, expected)
  }

  case object Top extends Type
  case object Bottom extends Type
  
  case class Const(value: Data) extends Type

  sealed trait PrimitiveType extends Type
  case object Null extends PrimitiveType
  case object Str extends PrimitiveType
  case object Int extends PrimitiveType
  case object Dec extends PrimitiveType
  case object Bool extends PrimitiveType
  case object Binary extends PrimitiveType
  case object DateTime extends PrimitiveType
  case object Interval extends PrimitiveType

  case class HomSet(value: Type) extends Type

  case class HomArray(value: Type) extends Type
  case class ArrayElem(index: Int, value: Type) extends Type

  case class HomObject(value: Type) extends Type
  case class ObjField(name: String, value: Type) extends Type

  case class IsOnly(value: Type) extends Type

  case class Product(left: Type, right: Type) extends Type {
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

  private def unifyWithProduct(expected: Type, product: Product): ValidationNel[TypeError, Type] = {
    unifyDucts(expected :: Nil, product.flatten, exists _)(Type.TypeAndMonoid)
  }

  private def unifyWithCoproduct(expected: Type, coproduct: Coproduct): ValidationNel[TypeError, Type] = {
    unifyDucts(expected :: Nil, coproduct.flatten, forall _)(Type.TypeOrMonoid)
  }

  private def exists(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Type] = actuals.headOption match {
    case Some(head) => typecheck(expected, head) ||| exists(expected, actuals.tail)
    case None => fail(expected, Product(actuals))
  }

  private def forall(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Type] = {
    implicit val m: Monoid[Type] = Type.TypeOrMonoid

    actuals.headOption match {
      case Some(head) => typecheck(expected, head) +++ exists(expected, actuals.tail)
      case None => Validation.success(Top)
    }
  }

  private def unifyDucts(expecteds: Seq[Type], actuals: Seq[Type], 
            check: (Type, Seq[Type]) => ValidationNel[TypeError, Type])(implicit m: Monoid[Type]) = {
    expecteds.foldLeft[ValidationNel[TypeError, Type]](Validation.success(Top)) {
      case (acc, expected) => {
        acc +++ check(expected, actuals)
      }
    }
  }

  def makeArray(values: Seq[Type]): Type = {
    Product(values.zipWithIndex.map(t => ArrayElem(t._2, t._1)))
  }

  val AnyArray = HomArray(Top)

  val AnyObject = HomObject(Top)

  val Numeric = Int | Dec
}