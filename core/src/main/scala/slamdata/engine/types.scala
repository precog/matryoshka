package slamdata.engine

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

  final def contains(that: Type): Boolean = Type.typecheck(self, that).fold(κ(false), κ(true))

  final def objectType: Option[Type] = this match {
    case Const(value) => value.dataType.objectType
    case AnonField(tpe) => Some(tpe)
    case NamedField(_, tpe) => Some(tpe)
    case x : Product => x.flatten.toList.map(_.objectType).sequenceU.map(types => types.reduce(Type.lub _))
    case x : Coproduct => x.flatten.toList.map(_.objectType).sequenceU.map(types => types.reduce(Type.lub _))
    case _ => None
  }

  final def objectLike: Boolean = this match {
    case Const(value) => value.dataType.objectLike
    case AnonField(_) => true
    case NamedField(_, _) => true
    case x : Product => x.flatten.toList.exists(_.objectLike)
    case x : Coproduct => x.flatten.toList.forall(_.objectLike)
    case _ => false
  }

  final def arrayType: Option[Type] = this match {
    case Const(value) => value.dataType.arrayType
    case AnonElem(tpe) => Some(tpe)
    case IndexedElem(_, tpe) => Some(tpe)
    case x : Product => x.flatten.toList.map(_.arrayType).sequenceU.map(types => types.reduce(Type.lub _))
    case x : Coproduct => x.flatten.toList.map(_.arrayType).sequenceU.map(types => types.reduce(Type.lub _))
    case _ => None
  }

  final def arrayLike: Boolean = this match {
    case Const(value) => value.dataType.arrayLike
    case AnonElem(_) => true
    case IndexedElem(_, _) => true
    case x : Product => x.flatten.toList.exists(_.arrayLike)
    case x : Coproduct => x.flatten.toList.forall(_.arrayLike)
    case _ => false
  }

  final def setLike: Boolean = this match {
    case Const(value) => value.dataType.setLike
    case Set(_) => true
    case _ => false
  }

  final def objectField(field: Type): ValidationNel[SemanticError, Type] = {
    val missingFieldType = Top | Bottom

    if (Type.lub(field, Str) != Str) failure(nel(TypeError(Str, field), Nil))
    else (field, this) match {
      case (Str, Const(Data.Obj(map))) => success(map.values.map(_.dataType).foldLeft[Type](Bottom)(Type.lub _))
      case (Const(Data.Str(field)), Const(Data.Obj(map))) =>
        // TODO: import toSuccess as method on Option (via ToOptionOps)?
        toSuccess(map.get(field).map(Const(_)))(nel(MissingField(field), Nil))

      case (Str, AnonField(value)) => success(value)
      case (Const(Data.Str(_)), AnonField(value)) => success(value)

      case (Str, NamedField(name, value)) => success(value | missingFieldType)
      case (Const(Data.Str(field)), NamedField(name, value)) =>
        success(if (field == name) value else missingFieldType)

      case (_, x : Product) => {
        // Note: this is not simple recursion because of the way we interpret NamedField types
        // when the field name doesn't match. Any number of those can be present, and are ignored,
        // unless there is nothing else.
        // Still, there's certainly a more elegant way.
        val v = x.flatten.toList.flatMap( t => {
          val ot = t.objectField(field)
          t match {
            case NamedField(_, _) if (ot == success(missingFieldType)) => Nil
            case _ => ot :: Nil
          }
        })
        v match {
          case Nil => success(missingFieldType)
          case _ => {
            implicit val and = Type.TypeAndMonoid
            v.reduce(_ +++ _)
          }
        }
      }

      case (_, x : Coproduct) => {
        // Note: this is not simple recursion because of the way we interpret NamedField types
        // when the field name doesn't match. Any number of those can be present, and are ignored,
        // unless there is nothing else.
        // Still, there's certainly a more elegant way.
        val v = x.flatten.toList.flatMap( t => {
          val ot = t.objectField(field)
          t match {
            case NamedField(_, _) if (ot == success(missingFieldType)) => Nil
            case _ => ot :: Nil
          }
        })
        v match {
          case Nil => success(missingFieldType)
          case _ => {
            implicit val or = Type.TypeOrMonoid
            (success(missingFieldType) :: v).reduce(_ +++ _)
          }
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

      case (Int, AnonElem(value)) => success(value)
      case (Const(Data.Int(_)), AnonElem(value)) => success(value)

      case (Int, IndexedElem(_, value)) => success(value)
      case (Const(Data.Int(index1)), IndexedElem(index2, value)) if (index1.toInt == index2) => success(value)

      case (_, x : Product) =>
        // TODO: needs to ignore failures that are IndexedElems, similar to what's happening
      // in objectField.
        implicit val or = TypeOrMonoid
        x.flatten.toList.map(_.arrayElem(index)).reduce(_ +++ _)

      case (_, x : Coproduct) =>
        implicit val lub = Type.TypeLubSemigroup
        x.flatten.toList.map(_.arrayElem(index)).reduce(_ +++ _)

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

  val TypeLubSemigroup = new Semigroup[Type] {
    def append(f1: Type, f2: => Type): Type = Type.lub(f1, f2)
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

      case (AnonField(superType), AnonField(subType)) => typecheck(superType, subType)

      case (AnonField(superType), NamedField(name, subType)) => typecheck(superType, subType)
      case (NamedField(name, superType), AnonField(subType)) => typecheck(superType, subType)

      case (NamedField(name1, superType), NamedField(name2, subType)) if (name1 == name2) => typecheck(superType, subType)

      case (AnonElem(superType), AnonElem(subType)) => typecheck(superType, subType)

      case (AnonElem(superType), IndexedElem(idx, subType)) => typecheck(superType, subType)
      case (IndexedElem(idx, superType), AnonElem(subType)) => typecheck(superType, subType)

      case (IndexedElem(idx1, superType), IndexedElem(idx2, subType)) if (idx1 == idx2) => typecheck(superType, subType)

      case (Set(superType), Set(subType)) => typecheck(superType, subType)

      case (superType, subType @ Coproduct(_, _)) => typecheckPC(superType :: Nil, subType.flatten)

      case (superType @ Coproduct(_, _), subType) => typecheckCP(superType.flatten, subType :: Nil)

      case (superType, subType @ Product(_, _)) => typecheckPP(superType :: Nil, subType.flatten)

      case (superType @ Product(_, _), subType) => typecheckPP(superType.flatten, subType :: Nil)

      case _ => fail(superType, subType)
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
    case AnonElem(value) => value :: Nil
    case IndexedElem(index, value) => value :: Nil
    case AnonField(value) => value :: Nil
    case NamedField(name, value) => value :: Nil
    case x : Product => x.flatten.toList
    case x : Coproduct => x.flatten.toList
  }

  def foldMap[Z: Monoid](f: Type => Z)(v: Type): Z = Monoid[Z].append(f(v), Foldable[List].foldMap(children(v))(foldMap(f)))

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
      case AnonElem(value)          => wrap(value, AnonElem)
      case IndexedElem(idx, value)  => wrap(value, IndexedElem(idx, _))
      case AnonField(value)         => wrap(value, AnonField)
      case NamedField(name, value)  => wrap(value, NamedField(name, _))

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

  case class AnonElem(value: Type) extends Type
  case class IndexedElem(index: Int, value: Type) extends Type

  case class AnonField(value: Type) extends Type
  case class NamedField(name: String, value: Type) extends Type

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

  def makeObject(values: Iterable[(String, Type)]) = Product.apply(values.toList.map((NamedField.apply _).tupled))

  def makeArray(values: List[Type]): Type = {
    val consts = values.collect { case Const(data) => data }

    if (consts.length == values.length) Const(Data.Arr(consts))
    else Product(values.zipWithIndex.map(t => IndexedElem(t._2, t._1)))
  }

  val AnyArray = AnonElem(Top)

  val AnyObject = AnonField(Top)

  val AnySet = Set(Top)

  val Numeric = Int | Dec

  val Temporal = Timestamp | Date | Time | Interval

  val Comparable = Numeric | Str | Temporal | Bool
}
