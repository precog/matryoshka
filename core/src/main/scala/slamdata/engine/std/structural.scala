package slamdata.engine.std

import scalaz._
import Scalaz._
import Validation.{success, failure}
import NonEmptyList.nel

import slamdata.engine._; import LogicalPlan._

import SemanticError._

trait StructuralLib extends Library {
  import Type._

  val MakeObject = Mapping(
    "MAKE_OBJECT",
    "Makes a singleton object containing a single field",
    Str :: Top :: Nil,
    noSimplification,
    partialTyper {
      case List(Const(Data.Str(name)), Const(data)) => Const(Data.Obj(Map(name -> data)))
      case List(Const(Data.Str(name)), valueType)   => Obj(Map(name -> valueType), None)
      case List(_, valueType)   => Obj(Map(), Some(valueType))
    },
    partialUntyperV(AnyObject) {
      case Const(Data.Obj(map)) => map.headOption match {
        case Some((key, value)) => success(List(Const(Data.Str(key)), Const(value)))
        case None => failure(NonEmptyList(GenericError("MAKE_OBJECT can’t result in an empty object")))
      }
      case Obj(map, uk) => map.headOption.fold(
        uk.fold[ValidationNel[SemanticError, List[Type]]](
          failure(NonEmptyList(GenericError("MAKE_OBJECT can’t result in an empty object"))))(
          t => success(List(Str, t)))) {
        case (key, value) => success(List(Const(Data.Str(key)), value))
      }
    })

  val MakeArray = Mapping(
    "MAKE_ARRAY",
    "Makes a singleton array containing a single element",
    Top :: Nil,
    noSimplification,
    partialTyper {
      case Const(data) :: Nil => Const(Data.Arr(data :: Nil))
      case valueType :: Nil   => Arr(List(valueType))
    },
    partialUntyper(AnyArray) {
      case Const(Data.Arr(List(elem))) => List(Const(elem))
      case Arr(List(elemType))         => List(elemType)
      case FlexArr(_, _, elemType)     => List(elemType)
    })

  val ObjectConcat: Mapping = Mapping(
    "OBJECT_CONCAT",
    "A right-biased merge of two objects into one object",
    AnyObject :: AnyObject :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Obj(map1)), Const(Data.Obj(map2))) =>
        success(Const(Data.Obj(map1 ++ map2)))
      case List(Const(o1 @ Data.Obj(_)), o2) => ObjectConcat(o1.dataType, o2)
      case List(o1, Const(o2 @ Data.Obj(_))) => ObjectConcat(o1, o2.dataType)
      case List(Obj(map1, uk1), Obj(map2, None)) =>
        success(Obj(map1 ++ map2, uk1))
      case List(Obj(map1, uk1), Obj(map2, Some(uk2))) =>
        success(Obj(
          map1 ∘ (Coproduct(_, uk2)) ++ map2,
          Some(uk1.fold(uk2)(Coproduct(_, uk2)))))
    },
    partialUntyper(AnyObject) {
      case x if x.objectLike =>
        val t = Obj(Map(), x.objectType)
        List(t, t)
    })

  val ArrayConcat: Mapping = Mapping(
    "ARRAY_CONCAT",
    "A merge of two arrays into one array",
    AnyArray :: AnyArray :: Nil,
    noSimplification,
    partialTyperV {
      case List(Const(Data.Arr(els1)), Const(Data.Arr(els2))) =>
        success(Const(Data.Arr(els1 ++ els2)))
      case List(Arr(els1), Arr(els2)) => success(Arr(els1 ++ els2))
      case List(Const(a1 @ Data.Arr(_)), a2) => ArrayConcat(a1.dataType, a2)
      case List(a1, Const(a2 @ Data.Arr(_))) => ArrayConcat(a1, a2.dataType)
      case List(a1, FlexArr(min2, max2, elem2)) =>
        (a1.arrayMinLength |@| a1.arrayType)((min1, typ1) =>
          success(FlexArr(
            min1 + min2,
            (a1.arrayMaxLength |@| max2)(_ + _),
            Type.lub(typ1, elem2))))
          .getOrElse(failure(NonEmptyList(GenericError(a1.toString + " is not an array."))))
      case List(FlexArr(min1, max1, elem1), a2) =>
        (a2.arrayMinLength |@| a2.arrayType)((min2, typ2) =>
          success(FlexArr(
            min1 + min2,
            (max1 |@| a2.arrayMaxLength)(_ + _),
            Type.lub(elem1, typ2))))
          .getOrElse(failure(NonEmptyList(GenericError(a2.toString + " is not an array."))))
    },
    partialUntyperV(AnyArray) {
      case x if x.arrayLike =>
        x.arrayType.fold[ValidationNel[SemanticError, List[Type]]](
          failure(NonEmptyList(GenericError("internal error: " + x.toString + " is arrayLike, but no arrayType")))) {
          typ =>
            val t = FlexArr(0, x.arrayMaxLength, typ)
            success(List(t, t))
        }
    })

  // NB: Used only during type-checking, and then compiled into either (string) Concat or ArrayConcat.
  val ConcatOp = Mapping(
    "(||)",
    "A merge of two arrays/strings.",
    (AnyArray | Str) :: (AnyArray | Str) :: Nil,
    noSimplification,
    partialTyperV {
      case t1 :: t2 :: Nil if (t1.arrayLike) && (t2 contains Top)    => success(t1 & FlexArr(0, None, Top))
      case t1 :: t2 :: Nil if (t1 contains Top) && (t2.arrayLike)    => success(FlexArr(0, None, Top) & t2)
      case t1 :: t2 :: Nil if (t1.arrayLike) && (t2.arrayLike)       => ArrayConcat(t1, t2)

      case Const(Data.Str(str1)) :: Const(Data.Str(str2)) :: Nil     => success(Const(Data.Str(str1 ++ str2)))
      case t1 :: t2 :: Nil if (Str contains t1) && (t2 contains Top) => success(Type.Str)
      case t1 :: t2 :: Nil if (t1 contains Top) && (Str contains t2) => success(Type.Str)
      case t1 :: t2 :: Nil if (Str contains t1) && (Str contains t2) => success(Type.Str)

      case t1 :: t2 :: Nil if t1 == t2 => success(t1)

      case t1 :: t2 :: Nil if (Str contains t1) && (t2.arrayLike) => failure(NonEmptyList(GenericError("cannot concat string with array")))
      case t1 :: t2 :: Nil if (t1.arrayLike) && (Str contains t2) => failure(NonEmptyList(GenericError("cannot concat array with string")))
    },
    partialUntyperV(AnyArray | Str) {
      case x if x contains (AnyArray | Str) => success((AnyArray | Str) :: (AnyArray | Str) :: Nil)
      case x if x.arrayLike                 => ArrayConcat.unapply(x)
      case Type.Str                         => success(Type.Str :: Type.Str :: Nil)
    })

  val ObjectProject = Mapping(
    "({})",
    "Extracts a specified field of an object",
    AnyObject :: Str :: Nil,
    noSimplification,
    partialTyperV { case List(v1, v2) => v1.objectField(v2) },
    x => success(Obj(Map(), Some(x)) :: Str :: Nil))

  val ArrayProject = Mapping(
    "([])",
    "Extracts a specified index of an array",
    AnyArray :: Int :: Nil,
    noSimplification,
    partialTyperV { case List(v1, v2) => v1.arrayElem(v2) },
    x => success(FlexArr(0, None, x) :: Int :: Nil) )

  val DeleteField: Mapping = Mapping(
    "DELETE_FIELD",
    "Deletes a specified field from an object",
    AnyObject :: Str :: Nil,
    noSimplification,
    partialTyper {
      case List(Const(Data.Obj(map)), Const(Data.Str(key))) =>
        Const(Data.Obj(map - key))
      case List(Obj(map, uk), Const(Data.Str(key))) => Obj(map - key, uk)
      case List(v1, _) => Obj(Map(), v1.objectType)
    },
    partialUntyperV(AnyObject) {
      case Const(o @ Data.Obj(map)) => DeleteField.unapply(o.dataType)
      case Obj(map, _)              => success(List(Obj(map, Some(Top)), Str))
    })

  val FlattenObject = ExpansionFlat(
    "FLATTEN_OBJECT",
    "Flattens an object into a set",
    AnyObject :: Nil,
    noSimplification,
    partialTyperV {
      case List(x) if x.objectLike =>
        x.objectType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: objectLike, but no objectType"))))(
          success)
    },
    tpe => success(List(Obj(Map(), Some(tpe)))))

  val FlattenArray = ExpansionFlat(
    "FLATTEN_ARRAY",
    "Flattens an array into a set",
    AnyArray :: Nil,
    noSimplification,
    partialTyperV {
      case List(x) if x.arrayLike =>
        x.arrayType.fold[ValidationNel[SemanticError, Type]](
          failure(NonEmptyList(GenericError("internal error: arrayLike, but no arrayType"))))(
          success)
    },
    tpe => success(List(FlexArr(0, None, tpe))))

  def functions = MakeObject :: MakeArray ::
                  ObjectConcat :: ArrayConcat :: ConcatOp ::
                  ObjectProject :: ArrayProject ::
                  FlattenObject :: FlattenArray ::
                  Nil

  // TODO: fix types and add the VirtualFuncs to the list of functions

  // val MakeObjectN = new VirtualFunc {
  object MakeObjectN {
    import slamdata.engine.analysis.fixplate._

    // Note: signature does not match VirtualFunc
    def apply(args: (Term[LogicalPlan], Term[LogicalPlan])*): Term[LogicalPlan] =
      args.map(t => MakeObject(t._1, t._2)) match {
        case t :: Nil => t
        case mas => mas.reduce((t, ma) => ObjectConcat(t, ma))
      }

    // Note: signature does not match VirtualFunc
    def unapply(t: Term[LogicalPlan]): Option[List[(Term[LogicalPlan], Term[LogicalPlan])]] =
      for {
        pairs <- Attr.unapply(attrK(t, ()))
      } yield pairs.map(_.bimap(forget(_), forget(_)))

    object Attr {
      // Note: signature does not match VirtualFuncAttrExtractor
      def unapply[A](t: Cofree[LogicalPlan, A]): Option[List[(Cofree[LogicalPlan, A], Cofree[LogicalPlan, A])]] = t.tail match {
        case MakeObject(name :: expr :: Nil) =>
          Some((name, expr) :: Nil)

        case ObjectConcat(a :: b :: Nil) =>
          (unapply(a) |@| unapply(b))(_ ::: _)

        case _ => None
      }
    }
  }

  val MakeArrayN: VirtualFunc = new VirtualFunc {
    import slamdata.engine.analysis.fixplate._

    def apply(args: Term[LogicalPlan]*): Term[LogicalPlan] =
      args.map(MakeArray(_)) match {
        case Nil      => LogicalPlan.Constant(Data.Arr(Nil))
        case t :: Nil => t
        case mas      => mas.reduce((t, ma) => ArrayConcat(t, ma))
      }

    def Attr = new VirtualFuncAttrExtractor {
      def unapply[A](t: Cofree[LogicalPlan, A]): Option[List[Cofree[LogicalPlan, A]]] = t.tail match {
        case MakeArray(x :: Nil) =>
          Some(x :: Nil)

        case ArrayConcat(a :: b :: Nil) =>
          (unapply(a) |@| unapply(b))(_ ::: _)

        case _ => None
      }
    }
  }
}
object StructuralLib extends StructuralLib
