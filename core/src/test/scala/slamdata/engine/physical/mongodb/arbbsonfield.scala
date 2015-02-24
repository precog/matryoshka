package slamdata.engine.physical.mongodb

import org.scalacheck._

trait ArbBsonField {
  lazy val genBsonFieldName = for {
    c   <- Gen.alphaChar
    str <- Gen.alphaStr
  } yield BsonField.Name(c.toString + str)

  lazy val genBsonFieldIndex = for {
    index <- Gen.chooseNum(0, 10)
  } yield BsonField.Index(index)

  implicit val arbBsonField: Arbitrary[BsonField] = Arbitrary(for {
    list <- Gen.nonEmptyListOf(Gen.oneOf(genBsonFieldName, genBsonFieldIndex))

    f = BsonField(list)

    if (!f.isEmpty)
  } yield f.get)
}
