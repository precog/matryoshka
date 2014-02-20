package slamdata.engine

import scalaz._

import scalaz.std.vector._
import scalaz.std.indexedSeq._
import scalaz.std.anyVal._

import SemanticError.TypeError

sealed trait Type { self =>
  import Type._

  final def & (that: Type) = Type.Product(this, that)

  final def | (that: Type) = Type.Coproduct(this, that)

  final def check: ValidationNel[TypeError, Unit] = {
    ???
  }

  final def mapUp(f: Type => Type): Type = {
    def loop(v: Type): Type = v match {
      case Top => f(v)
      case Bottom => f(v)
      case Const(value) =>
         val newType = f(value.dataType)

         if (newType != value.dataType) newType
         else f(newType)

      case Null => f(v)
      case Str => f(v)
      case Int => f(v)
      case Dec => f(v)
      case Bool => f(v)
      case Binary => f(v)
      case DateTime => f(v)
      case Interval => f(v)
      case HomSet(value) => f(loop(value))
      case HomArray(value) => f(loop(value))
      case ArrayElem(index, value) => f(loop(value))
      case HomObject(value) => f(loop(value))
      case ObjField(name, value) => f(loop(value))
      case IsOnly(value) => f(loop(value))
      case x : Product => f(Product(x.flatten.map(loop _)))
      case x : Coproduct => f(Coproduct(x.flatten.map(loop _)))
    }

    loop(self)
  }

  /**
   * Computes the least upper bound of any coproducts in this type.
   */
  final def lub: Type = mapUp {
    case x : Coproduct => x.flatten.reduce(Type.lub)

    case x => x
  }
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
  private def fail[A](expected: Type, actual: Type, message: Option[String]): ValidationNel[TypeError, A] = 
    Validation.failure(NonEmptyList(TypeError(expected, actual, message)))

  private def fail[A](expected: Type, actual: Type): ValidationNel[TypeError, A] = fail(expected, actual, None)

  private def fail[A](expected: Type, actual: Type, msg: String): ValidationNel[TypeError, A] = fail(expected, actual, Some(msg))

  private def succeed[A](v: A): ValidationNel[TypeError, A] = Validation.success(v)

  def simplify(tpe: Type): Type = tpe match {
    case x : Product => Product(x.flatten.toList.map(simplify _).filter(_ != Top).distinct)
    case x : Coproduct => Coproduct(x.flatten.toList.map(simplify _).distinct)
    case _ => tpe
  }

  def lub(left: Type, right: Type): Type = (left.lub, right.lub) match {
    case (Top, right) => right
    case (left, Top) => left

    case (Bottom, right) => Bottom
    case (left, Bottom) => Bottom

    case (Const(left), right) => lub(left.dataType, right)
    case (left, Const(right)) => lub(left, right.dataType)

    case (ObjField(name1, left), ObjField(name2, right)) =>
      if (name1 == name2) ObjField(name1, lub(left, right)) else HomObject(lub(left, right))
    case (HomObject(left), ObjField(name, right)) => HomObject(lub(left, right))
    case (ObjField(name, left), HomObject(right)) => HomObject(lub(left, right))

    case (ArrayElem(idx1, left), ArrayElem(idx2, right)) =>
      if (idx1 == idx2) ArrayElem(idx1, lub(left, right)) else HomArray(lub(left, right))
    case (HomArray(left), ArrayElem(idx, right)) => HomArray(lub(left, right))
    case (ArrayElem(idx, left), HomArray(right)) => HomArray(lub(left, right))

    case (left : Product, right) => left.flatten.map(term => lub(term, right)).reduce(_ & _)

    case (left, right : Product) => right.flatten.map(term => lub(left, term)).reduce(_ & _)

    case _ => Top
  }

  def typecheck(actual: Type, expected: Type): ValidationNel[TypeError, Unit] = (actual, expected) match {
    case (Top, expected) => succeed(Unit)

    case (actual, expected) if (actual == expected) => succeed(Unit)

    case (Const(_), _) => fail(actual, expected) // they're not equal so we can't unify

    case (actual, Const(data)) => typecheck(actual, data.dataType) // try to generalize the const

    case (expected : Product, actual : Product) => unifyDucts(expected.flatten, actual.flatten, exists _)

    case (expected : Product, actual : Coproduct) => unifyDucts(expected.flatten, actual.flatten, forall _)

    case (expected : Coproduct, actual : Product) => unifyDucts(expected.flatten, actual.flatten, forall _)

    case (expected : Coproduct, actual : Coproduct) => unifyDucts(expected.flatten, actual.flatten, exists _)

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
    def apply(values: Seq[Type]): Type = {
      if (values.length == 0) Top
      else if (values.length == 1) values.head
      else values.tail.foldLeft[Type](values.head)(_ & _)
    }
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
    def apply(values: Seq[Type]): Type = {
      if (values.length == 0) Bottom
      else if (values.length == 1) values.head
      else values.tail.foldLeft[Type](values.head)(_ | _)
    }
  }

  private def unifyWithProduct(expected: Type, product: Product): ValidationNel[TypeError, Unit] = {
    unifyDucts(expected :: Nil, product.flatten, exists _)
  }

  private def unifyWithCoproduct(expected: Type, coproduct: Coproduct): ValidationNel[TypeError, Unit] = {
    unifyDucts(expected :: Nil, coproduct.flatten, forall _)
  }

  private def exists(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = actuals.headOption match {
    case Some(head) => typecheck(expected, head) ||| exists(expected, actuals.tail)
    case None => fail(expected, Product(actuals))
  }

  private def forall(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = {
    actuals.headOption match {
      case Some(head) => typecheck(expected, head) +++ exists(expected, actuals.tail)
      case None => Validation.success(Top)
    }
  }

  private def unifyDucts(expecteds: Seq[Type], actuals: Seq[Type], check: (Type, Seq[Type]) => ValidationNel[TypeError, Unit]) = {
    expecteds.foldLeft[ValidationNel[TypeError, Unit]](Validation.success(Unit)) {
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