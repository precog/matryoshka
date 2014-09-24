package slamdata.engine.std

import scalaz._
import Scalaz._
import Validation.{success, failure}
import NonEmptyList.nel

import slamdata.engine._

import SemanticError._

trait StructuralLib extends Library {
  import Type._
  import Validation.{success, failure}

  val MakeObject = Mapping("MAKE_OBJECT", "Makes a singleton object containing a single field", Str :: Top :: Nil, partialTyper {
    case Const(Data.Str(name)) :: Const(data) :: Nil => Const(Data.Obj(Map(name -> data)))
    case Const(Data.Str(name)) :: (valueType) :: Nil => (NamedField(name, valueType))
    case _ :: Const(data) :: Nil => (AnonField(data.dataType))
    case _ :: valueType :: Nil => (AnonField(valueType))
  }, {
    case Const(Data.Obj(map)) => map.head match { case (key, value) => success(Const(Data.Str(key)) :: Const(value) :: Nil) }
    case NamedField(name, valueType) => success(Const(Data.Str(name)) :: valueType :: Nil)
    case AnonField(tpe) => success(Str :: tpe :: Nil)
    case t => failure(nel(TypeError(AnyObject, t), Nil))
  })

  val MakeArray = Mapping("MAKE_ARRAY", "Makes a singleton array containing a single element", Top :: Nil, partialTyper {
    case Const(data) :: Nil => Const(Data.Arr(data :: Nil))
    case (valueType) :: Nil => (AnonElem(valueType))
    case _ => AnyArray
  }, {
    case Const(Data.Arr(arr)) => success(Const(arr.head) :: Nil)    
    case AnonElem(elemType) => success(elemType :: Nil)
    case t => failure(nel(TypeError(AnyArray, t), Nil))
  })

  val ObjectConcat = Mapping("OBJECT_CONCAT", "A right-biased merge of two objects into one object", AnyObject :: AnyObject :: Nil, partialTyper {
    case Const(Data.Obj(map1)) :: Const(Data.Obj(map2)) :: Nil => Const(Data.Obj(map1 ++ map2))
    case v1 :: v2 :: Nil => (v1 & v2)
  }, {
    case x if x.objectLike => success(AnyObject :: AnyObject :: Nil)
    case x => failure(nel(TypeError(AnyObject, x), Nil))
  })

  val ArrayConcat = Mapping("ARRAY_CONCAT", "A right-biased merge of two arrays into one array", AnyArray :: AnyArray :: Nil, partialTyper {
    case Const(Data.Arr(els1)) :: Const(Data.Arr(els2)) :: Nil => Const(Data.Arr(els1 ++ els2))
    case v1 :: v2 :: Nil => (v1 & v2) // TODO: Unify het array into hom array
  }, {
    case x if x.arrayLike => success(AnyArray :: AnyArray :: Nil)
    case x => failure(nel(TypeError(AnyArray, x), Nil))
  })

  val ObjectProject = Mapping("({})", "Extracts a specified field of an object", AnyObject :: Str :: Nil, partialTyperV {
    case v1 :: v2 :: Nil => v1.objectField(v2)
  }, {    
    case x => success(AnonField(x) :: Str :: Nil)
  })

  val ArrayProject = Mapping("([])", "Extracts a specified index of an array", AnyArray :: Int :: Nil, partialTyperV {
    case v1 :: v2 :: Nil => v1.arrayElem(v2)
  }, {
    case x => success(AnonElem(x) :: Int :: Nil)
  })

  val FlattenObject = ExpansionFlat("FLATTEN_OBJECT", "Flattens an object into a set", AnyObject :: Nil, partialTyper {
    case x :: Nil if (!x.objectType.isEmpty) => x.objectType.get
  }, {
    case tpe => success(AnonField(tpe) :: Nil)
  })

  val FlattenArray = ExpansionFlat("FLATTEN_ARRAY", "Flattens an array into a set", AnyArray :: Nil, partialTyper {
    case x :: Nil if (!x.arrayType.isEmpty) => x.arrayType.get
  }, {
    case tpe => success(AnonElem(tpe) :: Nil)
  })

  val Squash = Squashing("SQUASH", "Squashes all dimensional information", Top :: Nil, partialTyper {
    case x :: Nil => x
  }, {
    case tpe => success(tpe :: Nil)
  })

  def functions = MakeObject :: MakeArray :: 
                  ObjectConcat :: ArrayConcat :: 
                  ObjectProject :: ArrayProject :: 
                  FlattenObject :: FlattenArray ::
                  Squash :: Nil

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
      } yield pairs.map { case (key, expr) => (forget(key), forget(expr)) }

    object Attr {
      import slamdata.engine.analysis.fixplate.{Attr => FAttr}
      
      // Note: signature does not match VirtualFuncAttrExtractor
      def unapply[A](t: FAttr[LogicalPlan, A]): Option[List[(FAttr[LogicalPlan, A], FAttr[LogicalPlan, A])]] = t.unFix.unAnn match {
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
        case t :: Nil => t
        case mas => mas.reduce((t, ma) => ArrayConcat(t, ma))
      }

    def Attr = new VirtualFuncAttrExtractor {
      import slamdata.engine.analysis.fixplate.{Attr => FAttr}

      def unapply[A](t: FAttr[LogicalPlan, A]): Option[List[FAttr[LogicalPlan, A]]] = t.unFix.unAnn match {
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
