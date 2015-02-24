package slamdata.engine.physical

import scalaz._

package object mongodb {

  case class NameGen(nameGen: Int)

  // used by State(T).runZero
  implicit val NameGenMonoid: Monoid[NameGen] = new Monoid[NameGen] {
    def zero = NameGen(0)
    def append(f1: NameGen, f2: => NameGen) = NameGen(f1.nameGen max f2.nameGen)
  }

  def freshId(label: String): State[NameGen, String] = for {
    n <- State((s: NameGen) => s.copy(nameGen = s.nameGen + 1) -> s.nameGen)
  } yield "__" + label + n.toString

  // TODO: parameterize over label (#510)
  def freshName: State[NameGen, BsonField.Name] =
    freshId("tmp").map(BsonField.Name(_))}
