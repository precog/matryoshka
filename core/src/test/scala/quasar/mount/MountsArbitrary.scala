package quasar.mount

import org.scalacheck._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.std.list._
import scalaz.syntax.std.option._

object MountsArbitrary {
  import Arbitrary.arbitrary

  implicit def mountsArbitrary[A: Arbitrary]: Arbitrary[Mounts[A]] =
    Arbitrary(for {
      n    <- Gen.size
      x    <- Gen.choose(0, n)
      dirs <- Gen.listOfN(x, arbitrary[RelDir[Sandboxed]])
      uniq =  dirs.zipWithIndex map { case (d, i) =>
                rootDir </> dir(i.toString) </> d
              }
      as   <- Gen.listOfN(x, arbitrary[A])
      mres =  Mounts.fromFoldable(uniq zip as)
      mnts <- mres.toOption.cata(Gen.const, Gen.fail)
    } yield mnts)
}
