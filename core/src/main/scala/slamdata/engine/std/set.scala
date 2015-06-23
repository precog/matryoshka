package slamdata.engine.std

import scalaz._
import Validation.{success, failure}
import NonEmptyList.nel

import slamdata.engine._; import LogicalPlan._
import slamdata.engine.analysis.fixplate._

import scalaz.syntax.applicative._

trait SetLib extends Library {
  val Take = Transformation("TAKE", "Takes the first N elements from a set", Type.Top :: Type.Int :: Nil,
    partialSimplifier {
      case List(_, Term(ConstantF(Data.Int(n)))) if n == 0 =>
        Constant(Data.Set(Nil))
    },
    partialTyper {
      case _ :: Type.Const(Data.Int(n)) :: Nil if n == 0 =>
        Type.Const(Data.Set(Nil))
      case Type.Set(t) :: _ :: Nil => t
      case t           :: _ :: Nil => t
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Int :: Nil)
      case t           => success(Type.Set(t) :: Type.Int :: Nil)
    })

  val Drop = Transformation("DROP", "Drops the first N elements from a set", Type.Top :: Type.Int :: Nil,
    partialSimplifier {
      case List(set, Term(ConstantF(Data.Int(n)))) if n == 0 => set
    },
    partialTyper {
      case Type.Set(t) :: _ :: Nil => t
      case t           :: _ :: Nil => t
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Int :: Nil)
      case t           => success(Type.Set(t) :: Type.Int :: Nil)
    })

  val OrderBy = Transformation("ORDER BY", "Orders a set by the natural ordering of a projection on the set", Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case set :: by :: Nil => set
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Top :: Nil)
      case t => success(Type.Set(t) :: Type.Top :: Nil)
    })

  val Filter = Transformation("WHERE", "Filters a set to include only elements where a projection is true", Type.Top :: Type.Bool :: Nil,
    partialSimplifier {
      case List(set, Term(ConstantF(Data.True))) => set
      case List(_, Term(ConstantF(Data.False))) => Constant(Data.Set(Nil))
    },
    partialTyper {
      case _   :: Type.Const(Data.False) :: Nil => Type.Const(Data.Set(Nil))
      case set :: by                     :: Nil => set
    },
    t => success(t :: Type.Bool :: Nil))

  val Cross = Transformation(
    "CROSS",
    "Computes the Cartesian product of two sets",
    Type.Top :: Type.Top :: Nil,
    partialSimplifier {
      case List(Term(ConstantF(Data.Set(Nil))), _) => Constant(Data.Set(Nil))
      case List(_, Term(ConstantF(Data.Set(Nil)))) => Constant(Data.Set(Nil))
    },
    partialTyper {
      case List(s1, s2) => Type.Obj(Map("left" -> s1, "right" -> s2), None)
    },
    t => (t.objectField(Type.Const(Data.Str("left"))) |@| t.objectField(Type.Const(Data.Str("right"))))(_ :: _ :: Nil))

  val GroupBy = Transformation("GROUP BY", "Groups a projection of a set by another projection", Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper {
      case s1 :: s2 :: Nil => s1
    },
    {
      case Type.Set(t) => success(Type.Set(t) :: Type.Top :: Nil)
      case t           => success(Type.Set(t) :: Type.Top :: Nil)
    })

  val Distinct = Transformation("DISTINCT", "Discards all but the first instance of each unique value",
    Type.Top :: Nil,
    noSimplification,
    partialTyper { case a :: Nil => a},
    x => success(x :: Nil))

  val DistinctBy = Transformation("DISTINCT BY", "Discards all but the first instance of the first argument, based on uniqueness of the second argument",
    Type.Top :: Type.Top :: Nil,
    noSimplification,
    partialTyper { case a :: _ :: Nil => a },
    x => success(x :: Type.Top :: Nil))

  def functions = Take :: Drop :: OrderBy :: Filter :: Cross :: GroupBy :: Distinct :: DistinctBy :: Nil
}
object SetLib extends SetLib
