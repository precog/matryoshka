package quasar.fs.mount

import quasar.Predef.List

import org.specs2.mutable
import org.specs2.ScalaCheck
import org.specs2.scalaz.DisjunctionMatchers
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.std.list._

class MountsSpec extends mutable.Specification with ScalaCheck with DisjunctionMatchers {
  "Mounts" should {
    "adding entries" >> {
      "fails when dir is a prefix of existing" ! prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt </> dir("c1"), 1).add(mnt, 2) must beLeftDisjunction
      }

      "fails when dir is prefixed by existing" ! prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt, 1).add(mnt </> dir("c2"), 2) must beLeftDisjunction
      }

      "succeeds when dir not a prefix of existing" >> {
        val mnt1: AbsDir[Sandboxed] = rootDir </> dir("one")
        val mnt2: AbsDir[Sandboxed] = rootDir </> dir("two")

        Mounts.fromFoldable(List((mnt1, 1), (mnt2, 2))) must beRightDisjunction
      }

      "succeeds when replacing value at existing" ! prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt, 1)
          .add(mnt, 2)
          .toOption
          .flatMap(_.toMap.get(mnt)) must beSome(2)
      }
    }
  }
}
