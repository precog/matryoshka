package quasar

import quasar.Predef.{Set => _, _}
import org.scalacheck._
import Gen._
import org.threeten.bp.{Instant, LocalDate, LocalTime, Duration}

import Type._

trait TypeGen {
  implicit def arbitraryType: Arbitrary[Type] = Arbitrary { Gen.sized(depth => typeGen(depth/25)) }

  def arbitrarySimpleType = Arbitrary { Gen.sized(depth => complexGen(depth/25, simpleGen)) }

  def arbitraryTerminal = Arbitrary { terminalGen }

  def arbitraryConst = Arbitrary { constGen }

  def arbitraryNonnestedType = Arbitrary { Gen.oneOf(Gen.const(Top), Gen.const(Bottom), simpleGen) }

  def typeGen(depth: Int): Gen[Type] = {
    // NB: never nests Top or Bottom inside any complex type, because that's mostly nonsensical.
    val gens = Gen.oneOf(Top, Bottom) :: List(terminalGen, constGen, objectGen, arrayGen).map(complexGen(depth, _))

    Gen.oneOf(gens(0), gens(1), gens.drop(2): _*)
  }

  def complexGen(depth: Int, gen: Gen[Type]): Gen[Type] =
    if (depth > 1) coproductGen(depth, gen)
    else gen

  def coproductGen(depth: Int, gen: Gen[Type]): Gen[Type] = for {
    left <- complexGen(depth-1, gen)
    right <- complexGen(depth-1, gen)
  } yield left | right

  def simpleGen: Gen[Type] = Gen.oneOf(terminalGen, constGen, setGen)

  def terminalGen: Gen[Type] = Gen.oneOf(Null, Str, Type.Int, Dec, Bool, Binary, Timestamp, Date, Time, Interval)

  def constGen: Gen[Type] =
    Gen.oneOf(Const(Data.Null), Const(Data.Str("a")), Const(Data.Int(1)),
              Const(Data.Dec(1.0)), Const(Data.True), Const(Data.Binary(Array[Byte](1))),
              Const(Data.Timestamp(Instant.now())),
              Const(Data.Date(LocalDate.now())),
              Const(Data.Time(LocalTime.now())),
              Const(Data.Interval(Duration.ofSeconds(1))))

  // TODO: can a Set contain constants? objects? arrays?
  def setGen: Gen[Type] = for {
    t <- terminalGen
  } yield Set(t)

  def fieldGen: Gen[(String, Type)] = for {
    c <- Gen.alphaChar
    t <- Gen.oneOf(terminalGen, constGen)
  } yield (c.toString(), t)

  def objectGen: Gen[Type] = for {
    t <- Gen.listOf(fieldGen)
    u <- Gen.oneOf[Option[Type]](None, Gen.oneOf(terminalGen, constGen).map(Some(_)))
  } yield Obj(t.toMap, u)

  def arrGen: Gen[Type] = for {
    t <- Gen.listOf(Gen.oneOf(terminalGen, constGen))
  } yield Arr(t)

  def flexArrayGen: Gen[Type] = for {
    i <- Gen.chooseNum(0, 10)
    n <- Gen.oneOf[Option[Int]](None, Gen.chooseNum(i, 20).map(Some(_)))
    t <- Gen.oneOf(terminalGen, constGen)
  } yield FlexArr(i, n, t)

  def arrayGen: Gen[Type] = Gen.oneOf(arrGen, flexArrayGen)
}

object TypeGen extends TypeGen
