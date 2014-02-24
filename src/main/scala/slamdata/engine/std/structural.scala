package slamdata.engine.std

import scalaz._

import slamdata.engine.{Mapping, Data, SemanticError, Type}

import Type._
import SemanticError._

trait StructuralLib extends Library {
  val MakeObject = Mapping("MAKE_OBJECT", "Makes a singleton object containing a single field", Str :: Top :: Nil, partialRefiner {
    case Type.Const(Data.Str(name)) :: Type.Const(data) :: Nil => Type.Const(Data.Obj(Map(name -> data)))
    case Type.Const(Data.Str(name)) :: (valueType) :: Nil => (ObjField(name, valueType))
    case _ :: Type.Const(data) :: Nil => (HomObject(data.dataType))
    case _ :: valueType :: Nil => (HomObject(valueType))
  })

  val MakeArray = Mapping("MAKE_ARRAY", "Makes a singleton array containing a single element", Top :: Nil, partialRefiner {
    case Type.Const(data) :: Nil => Type.Const(Data.Arr(data :: Nil))
    case (valueType) :: Nil => (HomArray(valueType))
    case _ => (AnyArray)
  })

  val ObjectConcat = Mapping("OBJECT_CONCAT", "A right-biased merge of two objects into one object", AnyObject :: AnyObject :: Nil, partialRefiner {
    case Type.Const(Data.Obj(map1)) :: Type.Const(Data.Obj(map2)) :: Nil => Type.Const(Data.Obj(map1 ++ map2))
    case v1 :: v2 :: Nil => (v1 & v2)
  })

  val ArrayConcat = Mapping("ARRAY_CONCAT", "A right-biased merge of two arrays into one array", AnyArray :: AnyArray :: Nil, partialRefiner {
    case Type.Const(Data.Arr(els1)) :: Type.Const(Data.Arr(els2)) :: Nil => Type.Const(Data.Arr(els1 ++ els2))
    case v1 :: v2 :: Nil => (v1 & v2) // TODO: Unify het array into hom array
  })

  val ObjectProject = Mapping("({})", "Extracts a specified field of an object", AnyObject :: Str :: Nil, partialRefinerV {
    case Type.Const(Data.Obj(map)) :: Type.Const(Data.Str(name)) :: Nil => 
      map.get(name).map(Type.Const).map(Validation.success).getOrElse(
        Validation.failure(NonEmptyList(GenericError("The object " + map + " does not have the field " + name)))
      )
    case v1 :: Type.Const(Data.Str(name)) :: Nil => Validation.success((Top)) // TODO: See if we can infer type based on field name
    case v1 :: v2 :: Nil => Validation.success((Top))
  })

  val ArrayProject = Mapping("([])", "Extracts a specified index of an array", AnyArray :: Int :: Nil, partialRefinerV {
    case Type.Const(Data.Arr(els)) :: Type.Const(Data.Int(idx)) :: Nil => 
      // TODO: Don't use toInt, it's unsafe
      els.lift(idx.toInt).map(Type.Const).map(Validation.success).getOrElse(
        Validation.failure(NonEmptyList(GenericError("The array " + els + " does not have the element " + idx)))
      )
    case v1 :: Type.Const(Data.Int(idx)) :: Nil => Validation.success((Top)) // TODO: See if we can infer type based on idx
    case v1 :: v2 :: Nil => Validation.success((Top))
  })

  val DeleteField = Mapping("DELETE_FIELD", "Deletes a field inside an object", AnyObject :: Str :: Nil, partialRefiner {
    case Type.Const(Data.Obj(map)) :: Type.Const(Data.Str(name)) :: Nil => Type.Const(Data.Obj(map - name))
    case v1 :: Type.Const(Data.Str(name)) :: Nil => (Top) // TODO: See if we can infer type based on field name
    case v1 :: v2 :: Nil => (Top)
  })

  val DeleteIndex = Mapping("DELETE_INDEX", "Deletes an element inside an array", AnyArray :: Int :: Nil, partialRefiner {
    case Type.Const(Data.Arr(els)) :: Type.Const(Data.Int(idx)) :: Nil => 
      val index = idx.toInt
      Type.Const(Data.Arr(els.take(index) ++ els.drop(index + 1)))

    case v1 :: Type.Const(Data.Int(idx)) :: Nil => (Top) // TODO: See if we can infer type based on idx
    case v1 :: v2 :: Nil => (Top)
  })

  def functions = MakeObject :: MakeArray :: 
                  ObjectConcat :: ArrayConcat :: 
                  ObjectProject :: ArrayProject :: 
                  DeleteField :: DeleteIndex :: Nil
}
object StructuralLib extends StructuralLib