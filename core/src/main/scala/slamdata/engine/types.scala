package slamdata.engine

import collection.immutable.Map

import scalaz._
import Scalaz._

import slamdata.engine.fp._

import SemanticError.{TypeError, MissingField, MissingIndex}
import NonEmptyList.nel
import Validation.{success, failure}

sealed trait Type { self =>
  import Type._

  final def & (that: Type) = Type.Product(this, that)

  final def | (that: Type) = Type.Coproduct(this, that)

  final def lub: Type = mapUp(self) {
    case x : Coproduct => x.flatten.reduce(Type.lub)
  }

  final def glb: Type = mapUp(self) {
    case x : Coproduct => x.flatten.reduce(Type.glb)
  }

  final def contains(that: Type): Boolean =
    typecheck(self, that).fold(κ(false), κ(true))

  final def objectType: Option[Type] = this match {
    case Const(value) => value.dataType.objectType
    case Record(value, uk) =>
      val known = value.toList.map(_._2).concatenate(TypeOrMonoid)
        Some(uk.fold(known)(Type.glb(known, _)))
    case x : Product =>
      x.flatten.toList.map(_.objectType).sequence.map(_.concatenate(TypeAndMonoid))
    case x : Coproduct =>
      x.flatten.toList.map(_.objectType).sequence.map(_.concatenate(TypeOrMonoid))
    case _ => None
  }

  final def objectLike: Boolean = this match {
    case Const(value) => value.dataType.objectLike
    case Record(_, _) => true
    case x : Product => x.flatten.toList.exists(_.objectLike)
    case x : Coproduct => x.flatten.toList.forall(_.objectLike)
    case _ => false
  }

  final def arrayType: Option[Type] = this match {
    case Const(value) => value.dataType.arrayType
    case Arr(value) => Some(value.concatenate(TypeOrMonoid))
    case FlexArr(_, _, value) => Some(value)
    case x : Product =>
      x.flatten.toList.map(_.arrayType).sequenceU.map(_.reduce(Type.lub _))
    case x : Coproduct =>
      x.flatten.toList.map(_.arrayType).sequenceU.map(_.reduce(Type.lub _))
    case _ => None
  }

  final def arrayLike: Boolean = this match {
    case Const(value) => value.dataType.arrayLike
    case Arr(_) => true
    case FlexArr(_, _, _) => true
    case x : Product => x.flatten.toList.exists(_.arrayLike)
    case x : Coproduct => x.flatten.toList.forall(_.arrayLike)
    case _ => false
  }

  final def arrayMinLength: Option[Int] = this match {
    case Const(Data.Arr(value)) => Some(value.length)
    case Arr(value)             => Some(value.length)
    case FlexArr(minLen, _, _)  => Some(minLen)
    case x : Product =>
      x.flatten.toList.foldLeft[Option[Int]](Some(0))((a, n) =>
        (a |@| n.arrayMinLength)(_ max _))
    case x : Coproduct =>
      x.flatten.toList.foldLeft[Option[Int]](None)((a, n) =>
        (a |@| n.arrayMinLength)(_ min _))
    case _ => None
  }
  final def arrayMaxLength: Option[Int] = this match {
    case Const(Data.Arr(value)) => Some(value.length)
    case Arr(value)             => Some(value.length)
    case FlexArr(_, maxLen, _)  => maxLen
    case x : Product =>
      x.flatten.toList.foldLeft[Option[Int]](None)((a, n) =>
        (a |@| n.arrayMaxLength)(_ min _))
    case x : Coproduct =>
      x.flatten.toList.foldLeft[Option[Int]](Some(0))((a, n) =>
        (a |@| n.arrayMaxLength)(_ max _))
    case _ => None
  }

  final def setLike: Boolean = this match {
    case Const(value) => value.dataType.setLike
    case Set(_) => true
    case x : Product => x.flatten.toList.exists(_.setLike)
    case x : Coproduct => x.flatten.toList.forall(_.setLike)
    case _ => false
  }

  final def objectField(field: Type): ValidationNel[SemanticError, Type] = {
    if (Type.lub(field, Str) != Str) failure(nel(TypeError(Str, field), Nil))
    else (field, this) match {
      case (Str, Const(Data.Obj(map))) =>
        success(map.values.toList.foldMap(_.dataType)(TypeLubMonoid))
      case (Const(Data.Str(field)), Const(Data.Obj(map))) =>
        // TODO: import toSuccess as method on Option (via ToOptionOps)?
        toSuccess(map.get(field).map(Const(_)))(nel(MissingField(field), Nil))

      case (Str, r @ Record(_, _)) => success(r.objectType.get)
      case (Const(Data.Str(field)), Record(map, uk)) =>
        map.get(field).fold(uk.fold[ValidationNel[SemanticError, Type]](failure(nel(MissingField(field), Nil)))(success))(success)
      case (_, x : Product) =>
        implicit val and = Type.TypeAndMonoid
        x.flatten.foldMap(_.objectField(field))

      case (_, x : Coproduct) => {
        implicit val or = Type.TypeOrMonoid
        val rez = x.flatten.map(_.objectField(field))
        rez.foldMap(_.getOrElse(Bottom)) match {
          case x if simplify(x) == Bottom => rez.concatenate
          case x                          => success(x)
        }
      }

      case _ => failure(nel(TypeError(AnyObject, this), Nil))
    }
  }

  final def arrayElem(index: Type): ValidationNel[SemanticError, Type] = {
    if (Type.lub(index, Int) != Int) failure(nel(TypeError(Int, index), Nil))
    else (index, this) match {
      case (Const(Data.Int(index)), Const(Data.Arr(arr))) =>
        arr.lift(index.toInt).map(data => success(Const(data))).getOrElse(failure(nel(MissingIndex(index.toInt), Nil)))

      case (Int, Const(Data.Arr(arr))) => success(arr.map(_.dataType).reduce(_ | _))

      case (Int, FlexArr(_, _, value)) => success(value)
      case (Const(Data.Int(index)), FlexArr(min, max, value)) =>
        lazy val succ =
          success(value)
        max.fold[ValidationNel[SemanticError, Type]](
          succ)(
          max => if (index < max) succ else failure(nel(MissingIndex(index.toInt), Nil)))

      case (Int, a @ Arr(_)) => success(a.arrayType.get)
      case (Const(Data.Int(index)), a @ Arr(value)) =>
        if (index < value.length)
          success(a.value(index.toInt))
        else failure(nel(MissingIndex(index.toInt), Nil))

      case (_, x : Product) =>
        implicit val or = Type.TypeOrMonoid
        x.flatten.toList.foldMap(_.arrayElem(index))

      case (_, x : Coproduct) =>
        implicit val lub = Type.TypeLubMonoid
        x.flatten.toList.foldMap(_.arrayElem(index))

      case _ => failure(nel(TypeError(AnyArray, this), Nil))
    }
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

  val TypeGlbMonoid = new Monoid[Type] {
    def zero = Type.Top
    def append(f1: Type, f2: => Type) = Type.glb(f1, f2)
  }

  val TypeLubMonoid = new Monoid[Type] {
    def zero = Type.Bottom
    def append(f1: Type, f2: => Type) = Type.lub(f1, f2)
  }

  implicit val TypeRenderTree = new RenderTree[Type] {
    override def render(v: Type) = Terminal(v.toString, List("Type"))  // TODO
  }
}

case object Type extends TypeInstances {
  private def fail[A](expected: Type, actual: Type, message: Option[String]): ValidationNel[TypeError, A] =
    Validation.failure(NonEmptyList(TypeError(expected, actual, message)))

  private def fail[A](expected: Type, actual: Type): ValidationNel[TypeError, A] = fail(expected, actual, None)

  private def fail[A](expected: Type, actual: Type, msg: String): ValidationNel[TypeError, A] = fail(expected, actual, Some(msg))

  private def succeed[A](v: A): ValidationNel[TypeError, A] = Validation.success(v)

  def simplify(tpe: Type): Type = mapUp(tpe) {
    case x : Product => {
      val ts = x.flatten.toList.filter(_ != Top)
      if (ts.contains(Bottom)) Bottom else Product(ts.distinct)
    }
    case x : Coproduct => {
      val ts = x.flatten.toList.filter(_ != Bottom)
      if (ts.contains(Top)) Top else Coproduct(ts.distinct)
    }
    case x => x
  }

  def glb(left: Type, right: Type): Type = {
    if (left == right) left
    else if (left contains right) right
    else if (right contains left) left
    else Bottom
  }

  def lub(left: Type, right: Type): Type = (left, right) match {
    case _ if left == right       => left

    case _ if left contains right => left
    case _ if right contains left => right

    case (Const(l), Const(r))
      if l.dataType == r.dataType => l.dataType

    case _                        => Top
  }

  def typecheck(superType: Type, subType: Type):
      ValidationNel[TypeError, Unit] =
    (superType, subType) match {
      case (superType, subType) if (superType == subType) => succeed(Unit)

      case (Top, _)    => succeed(Unit)
      case (_, Bottom) => succeed(Unit)

      case (superType, Const(subType)) => typecheck(superType, subType.dataType)

      case (superType : Product, subType : Product) => typecheckPP(superType.flatten, subType.flatten)

      case (superType : Product, subType : Coproduct) => typecheckPC(superType.flatten, subType.flatten)

      case (superType : Coproduct, subType : Product) => typecheckCP(superType.flatten, subType.flatten)

      case (superType : Coproduct, subType : Coproduct) => typecheckCC(superType.flatten, subType.flatten)
      case (Arr(elem1), Arr(elem2)) =>
        if (elem1.length <= elem2.length)
          Zip[List].zipWith(elem1, elem2)(typecheck).concatenate
        else fail(superType, subType, "subtype must be at least as long")
      case (FlexArr(supMin, supMax, superType), Arr(elem2))
          if supMin <= elem2.length =>
        typecheck(superType, elem2.concatenate(TypeOrMonoid))
      case (FlexArr(supMin, supMax, superType), FlexArr(subMin, subMax, subType)) =>
        lazy val tc = typecheck(superType, subType)
        def checkOpt[A](sup: Option[A], comp: (A, A) => Boolean, sub: Option[A], next: => ValidationNel[TypeError, Unit]) =
          sup.fold(
            next)(
            p => sub.fold[ValidationNel[TypeError, Unit]](
              fail(superType, subType, "super/sub1"))(
              b => if (comp(p, b)) next else fail(superType, subType, "super/sub2")))
        lazy val max = checkOpt(supMax, Order[Int].greaterThanOrEqual, subMax, tc)
        checkOpt(Some(supMin), Order[Int].lessThanOrEqual, Some(subMin), max)
      case (Record(supMap, supUk), Record(subMap, subUk)) =>
        supMap.toList.foldMap { case (k, v) =>
          subMap.get(k).fold[ValidationNel[TypeError, Unit]](
            fail(superType, subType))(
            typecheck(v, _))
        } +++
          supUk.fold(
            subUk.fold[ValidationNel[TypeError, Unit]](
              if ((subMap -- supMap.keySet).isEmpty) succeed(Unit) else fail(superType, subType))(
              κ(fail(superType, subType))))(
            p => subUk.fold[ValidationNel[TypeError, Unit]](
              // if (subMap -- supMap.keySet) is empty, fail(superType, subType)
              (subMap -- supMap.keySet).foldMap(typecheck(p, _)))(
              typecheck(p, _)))

      case (Set(superType), Set(subType)) => typecheck(superType, subType)

      case (superType, subType @ Coproduct(_, _)) => typecheckPC(superType :: Nil, subType.flatten)

      case (superType @ Coproduct(_, _), subType) => typecheckCP(superType.flatten, subType :: Nil)

      case (superType, subType @ Product(_, _)) => typecheckPP(superType :: Nil, subType.flatten)

      case (superType @ Product(_, _), subType) => typecheckPP(superType.flatten, subType :: Nil)

      case _ => fail(superType, subType, "super/sub3")
    }

  def children(v: Type): List[Type] = v match {
    case Top => Nil
    case Bottom => Nil
    case Const(value) => value.dataType :: Nil
    case Null => Nil
    case Str => Nil
    case Int => Nil
    case Dec => Nil
    case Bool => Nil
    case Binary => Nil
    case Timestamp => Nil
    case Date => Nil
    case Time => Nil
    case Interval => Nil
    case Id => Nil
    case Set(value) => value :: Nil
    case Arr(value) => value
    case FlexArr(_, _, value) => value :: Nil
    case Record(map, uk) => uk.toList ++ map.values.toList
    case x : Product => x.flatten.toList
    case x : Coproduct => x.flatten.toList
  }

  def foldMap[Z: Monoid](f: Type => Z)(v: Type): Z =
    Monoid[Z].append(f(v), children(v).foldMap(foldMap(f)))

  def mapUp(v: Type)(f: PartialFunction[Type, Type]): Type = {
    val f0 = f.orElse[Type, Type] {
      case x => x
    }

    mapUpM[scalaz.Id.Id](v)(f0)
  }

  def mapUpM[F[_]: Monad](v: Type)(f: Type => F[Type]): F[Type] = {
    def loop(v: Type): F[Type] = v match {
      case Const(value) =>
         for {
          newType  <- f(value.dataType)
          newType2 <- if (newType != value.dataType) Monad[F].point(newType)
                      else f(v)
        } yield newType2

      case Set(value)               => wrap(value, Set)
      case FlexArr(min, max, value) => wrap(value, FlexArr(min, max, _))
      case Arr(value)               => value.map(f).sequence.map(Arr)
      case Record(map, uk)          =>
        ((map ∘ f).sequence |@| uk.map(f).sequence)(Record)

      case x : Product =>
        for {
          xs <- Traverse[List].sequence(x.flatten.toList.map(loop _))
          v2 <- f(Product(xs))
        } yield v2

      case x : Coproduct =>
        for {
          xs <- Traverse[List].sequence(x.flatten.toList.map(loop _))
          v2 <- f(Coproduct(xs))
        } yield v2

      case _ => f(v)
    }

    def wrap(v0: Type, constr: Type => Type) =
      for {
        v1 <- loop(v0)
        v2 <- f(constr(v1))
      } yield v2

    loop(v)
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
  case object Timestamp extends PrimitiveType
  case object Date extends PrimitiveType
  case object Time extends PrimitiveType
  case object Interval extends PrimitiveType
  case object Id extends PrimitiveType

  case class Set(value: Type) extends Type

  case class Arr(value: List[Type]) extends Type
  case class FlexArr(minSize: Int, maxSize: Option[Int], value: Type)
      extends Type

  case class Record(value: Map[String, Type], unknowns: Option[Type])
      extends Type

  case class Product(left: Type, right: Type) extends Type {
    def flatten: Vector[Type] = {
      def flatten0(v: Type): Vector[Type] = v match {
        case Product(left, right) => flatten0(left) ++ flatten0(right)
        case x => Vector(x)
      }

      flatten0(this)
    }

    override def hashCode = flatten.toSet.hashCode()

    override def equals(that: Any) = that match {
      case that : Product => this.flatten.toSet.equals(that.flatten.toSet)

      case _ => false
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

    override def hashCode = flatten.toSet.hashCode()

    override def equals(that: Any) = that match {
      case that : Coproduct => this.flatten.toSet.equals(that.flatten.toSet)

      case _ => false
    }
  }
  object Coproduct extends ((Type, Type) => Type) {
    def apply(values: Seq[Type]): Type = {
      if (values.length == 0) Bottom
      else if (values.length == 1) values.head
      else values.tail.foldLeft[Type](values.head)(_ | _)
    }
  }

  private def exists(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = actuals.headOption match {
    case Some(head) => typecheck(expected, head) ||| exists(expected, actuals.tail)
    case None => fail(expected, Product(actuals))
  }

  private def forall(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = {
    actuals.headOption match {
      case Some(head) => typecheck(expected, head) +++ forall(expected, actuals.tail)
      case None => Validation.success(Top)
    }
  }

  private val typecheckPP = typecheck(_ +++ _, exists _)

  private val typecheckPC = typecheck(_ +++ _, forall _)

  private val typecheckCP = typecheck(_ ||| _, exists _)

  private val typecheckCC = typecheck(_ ||| _, forall _)

  private def typecheck(combine: (ValidationNel[TypeError, Unit], ValidationNel[TypeError, Unit]) => ValidationNel[TypeError, Unit],
                        check: (Type, Seq[Type]) => ValidationNel[TypeError, Unit]) = (expecteds: Seq[Type], actuals: Seq[Type]) => {
    expecteds.foldLeft[ValidationNel[TypeError, Unit]](Validation.success(Unit)) {
      case (acc, expected) => {
        combine(acc, check(expected, actuals))
      }
    }
  }

  val AnyArray = FlexArr(0, None, Top)

  val AnyObject = Record(Map(), Some(Top))

  val AnySet = Set(Top)

  val Numeric = Int | Dec

  val Temporal = Timestamp | Date | Time | Interval

  val Comparable = Numeric | Str | Temporal | Bool
}
