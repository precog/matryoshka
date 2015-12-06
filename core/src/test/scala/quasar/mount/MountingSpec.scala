package quasar.mount

import quasar.Predef._
import quasar.Variables
import quasar.fp.prism._
import quasar.fs.{APath, ADir, AFile, PathError2, FileSystemType}
import quasar.recursionschemes.Fix
import quasar.specs2.DisjunctionMatchers
import quasar.sql

import monocle.function.Field1
import monocle.std.{disjunction => D}
import monocle.std.tuple2._
import org.specs2.execute._
import org.specs2.mutable
import org.specs2.specification._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

abstract class MountingSpec[S[_]](implicit S0: Functor[S], S1: MountingF :<: S)
  extends mutable.Specification with DisjunctionMatchers {

  import MountConfig2.{viewConfig, fileSystemConfig}

  def interpName: String
  def interpret: S ~> Task

  val mnt = Mounting.Ops[S]
  // NB: Without the explicit imports, scalac complains of an import cycle
  import mnt.{F, M, lookup, mountView, mountFileSystem, remount, replace, unmount}

  implicit class StrOps(s: String) {
    def >>*[A: AsResult](a: => F[A]): Example =
      s >> a.foldMap(interpret).run
  }

  val noVars   = Variables.fromMap(Map.empty)
  val exprA    = Fix(sql.StringLiteralF[sql.Expr]("A"))
  val exprB    = Fix(sql.StringLiteralF[sql.Expr]("B"))
  val viewCfgA = viewConfig(exprA, noVars)
  val viewCfgB = viewConfig(exprB, noVars)

  val dbType   = FileSystemType("db")
  val uriA     = ConnectionUri("db://example.com/A")
  val uriB     = ConnectionUri("db://example.com/B")
  val fsCfgA   = fileSystemConfig(dbType, uriA)
  val fsCfgB   = fileSystemConfig(dbType, uriB)

  val invalidPath = MountingError.pathError composePrism
                    PathError2.invalidPath  composeLens
                    Field1.first

  val notFound = MountingError.pathError composePrism
                 PathError2.pathNotFound

  val pathExists = MountingError.pathError composePrism
                   PathError2.pathExists

  def maybeInvalid[A](dj: MountingError \/ A): Option[APath] =
    D.left composeOptional invalidPath getOption dj

  def maybeNotFound[A](dj: MountingError \/ A): Option[APath] =
    D.left composePrism notFound getOption dj

  def maybeExists[A](dj: MountingError \/ A): Option[APath] =
    D.left composePrism pathExists getOption dj

  def mountViewNoVars(loc: AFile, query: sql.Expr): M[Unit] =
    mountView(loc, query, noVars)

  s"$interpName mounting interpreter" should {
    "lookup" >> {
      "returns a view config when asked for an existing view path" >>* {
        val f = rootDir </> dir("d1") </> file("f1")

        (mountViewNoVars(f, exprA).toOption *> lookup(f))
          .run map (_ must beSome(viewCfgA))
      }

      "returns a filesystem config when asked for an existing fs path" >>* {
        val d = rootDir </> dir("d1")

        (mountFileSystem(d, dbType, uriA).toOption *> lookup(d))
          .run map (_ must beSome(fsCfgA))
      }

      "returns none when nothing mounted at the requested path" >>* {
        val f = rootDir </> dir("d2") </> file("f2")
        val d = rootDir </> dir("d3")

        lookup(f).run.tuple(lookup(d).run)
          .map(_ must_== ((None, None)))
      }
    }

    "mountViewNoVars" >> {
      "allow mounting a view at a file path" >>* {
        val f = rootDir </> file("f1")

        (mountViewNoVars(f, exprA).toOption *> lookup(f))
          .run map (_ must beSome(viewCfgA))
      }

      "allow mounting a view above another view" >>* {
        val f1 = rootDir </> dir("d1") </> dir("d2") </> file("f1")
        val f2 = rootDir </> dir("d1") </> file("d2")

        val r = (
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB)
        ).toOption *> lookup(f2)

        r.run map (_ must beSome(viewCfgB))
      }

      "allow mounting a view below another view" >>* {
        val f1 = rootDir </> dir("d1") </> file("d2")
        val f2 = rootDir </> dir("d1") </> dir("d2") </> file("f2")

        val r = (
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB)
        ).toOption *> lookup(f2)

        r.run map (_ must beSome(viewCfgB))
      }

      "allow mounting a view above a filesystem" >>* {
        val d = rootDir </> dir("d1") </> dir("db")
        val f = rootDir </> file("d1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ).toOption *> lookup(f)

        r.run map (_ must beSome(viewCfgA))
      }

      "allow mounting a view at a file with the same name as an fs mount" >>* {
        val d = rootDir </> dir("d1")
        val f = rootDir </> file("d1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ).toOption *> lookup(f)

        r.run map (_ must beSome(viewCfgA))
      }

      "allow mounting a view below a filesystem" >>* {
        val d = rootDir </> dir("d1")
        val f = rootDir </> dir("d1") </> file("f1")

        val r = (
          mountFileSystem(d, dbType, uriA) *>
          mountViewNoVars(f, exprA)
        ).toOption *> lookup(f)

        r.run map (_ must beSome(viewCfgA))
      }

      "fail when a view is already mounted at the file path" >>* {
        val f = rootDir </> dir("d1") </> file("f1")

        (mountViewNoVars(f, exprA) *> mountViewNoVars(f, exprB)).run map { r =>
          maybeExists(r) must beSome(f)
        }
      }
    }

    "mountFileSystem" >> {
      def mountFF(d1: ADir, d2: ADir): M[Unit] =
        mountFileSystem(d1, dbType, uriA) *> mountFileSystem(d2, dbType, uriB)

      def mountVF(f: AFile, d: ADir): OptionT[F, MountConfig2] =
        (mountViewNoVars(f, exprA) *> mountFileSystem(d, dbType, uriA))
          .toOption *> lookup(d)

      "mounts a filesystem at a directory" >>* {
        val d = rootDir </> dir("d1")

        (mountFileSystem(d, dbType, uriA).toOption *> lookup(d))
          .run map (_ must beSome(fsCfgA))
      }

      "fail when mounting above an existing fs mount" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = d1 </> dir("d2")

        mountFF(d2, d1).run map (dj => maybeInvalid(dj) must beSome(d1))
      }

      "fail when mounting at an existing fs mount" >>* {
        val d = rootDir </> dir("exists")

        mountFF(d, d).run.tuple(lookup(d).run) map { case (dj, cfg) =>
          maybeExists(dj).tuple(cfg) must beSome((d, fsCfgA))
        }
      }

      "fail when mounting below an existing fs mount" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = d1 </> dir("d2")

        mountFF(d1, d2).run map (dj => maybeInvalid(dj) must beSome(d2))
      }

      "succeed when mounting above an existing view mount" >>* {
        val d = rootDir </> dir("d1")
        val f = d </> file("view")

        mountVF(f, d).run map (_ must beSome(fsCfgA))
      }

      "succeed when mounting at a dir with same name as existing view mount" >>* {
        val f = rootDir </> dir("d2") </> file("view")
        val d = rootDir </> dir("d2") </> dir("view")

        mountVF(f, d).run map (_ must beSome(fsCfgA))
      }

      "succeed when mounting below an existing view mount" >>* {
        val f = rootDir </> dir("d2") </> file("view")
        val d = rootDir </> dir("d2") </> dir("view") </> dir("db")

        mountVF(f, d).run map (_ must beSome(fsCfgA))
      }
    }

    "mount" >> {
      "succeeds when loc and config agree" >>* {
        val f = rootDir </> file("view")

        (mnt.mount(f, viewCfgA).toOption *> lookup(f))
          .run map (_ must beSome(viewCfgA))
      }

      "fails when using a dir for a view" >>* {
        mnt.mount(rootDir, viewCfgA)
          .run map (dj => maybeInvalid(dj) must beSome(rootDir[Sandboxed]))
      }

      "fails when using a file for a filesystem" >>* {
        val f = rootDir </> file("foo")
        mnt.mount(f, fsCfgA).run map (dj => maybeInvalid(dj) must beSome(f))
      }
    }

    "remount" >> {
      "moves the mount at src to dst" >>* {
        val d1 = rootDir </> dir("d1")
        val d2 = rootDir </> dir("d2")

        val r =
          (mnt.mount(d1, fsCfgA) *> remount(d1, d2))
            .run *> (lookup(d1).run.tuple(lookup(d2).run))

        r map (_ must_== ((None, Some(fsCfgA))))
      }

      "succeeds when src == dst" >>* {
        val d = rootDir </> dir("srcdst")

        val r =
          (mnt.mount(d, fsCfgB) *> remount(d, d))
            .toOption *> lookup(d)

        r.run map (_ must beSome(fsCfgB))
      }

      "fails if there is no mount at src" >>* {
        val d = rootDir </> dir("dne")

        remount(d, rootDir).run map (dj => maybeNotFound(dj) must beSome(d))
      }

      "restores the mount at src if mounting fails at dst" >>* {
        val f1 = rootDir </> file("f1")
        val f2 = rootDir </> file("f2")

        val r =
          mountViewNoVars(f1, exprA) *>
          mountViewNoVars(f2, exprB) *>
          remount(f1, f2)

        r.run.tuple(lookup(f1).run) map { case (dj, cfg) =>
          maybeExists(dj).tuple(cfg) must beSome((f2, viewCfgA))
        }
      }
    }

    "replace" >> {
      "replaces the mount at the location with a new one" >>* {
        val d = rootDir </> dir("replace")

        val r =
          (mnt.mount(d, fsCfgA) *> replace(d, fsCfgB))
            .toOption *> lookup(d)

        r.run map (_ must beSome(fsCfgB))
      }

      "fails if there is no mount at the given src location" >>* {
        val f = rootDir </> dir("dne") </> file("f1")

        replace(f, viewCfgA).run map (dj => maybeNotFound(dj) must beSome(f))
      }

      "restores the previous mount if mounting the new config fails" >>* {
        val f = rootDir </> file("f1")
        val r = mountViewNoVars(f, exprA) *> replace(f, fsCfgB)

        r.run.tuple(lookup(f).run) map { case (dj, cfg) =>
          maybeInvalid(dj).tuple(cfg) must beSome((f, viewCfgA))
        }
      }
    }

    "unmount" >> {
      "should remove an existing view mount" >>* {
        val f = rootDir </> file("tounmount")

        val r =
          (mountViewNoVars(f, exprA) *> unmount(f))
            .toOption *> lookup(f)

        r.run map (_ must beNone)
      }

      "should remove an existing fs mount" >>* {
        val d = rootDir </> dir("tounmount")

        val r =
          (mountFileSystem(d, dbType, uriB) *> unmount(d))
            .toOption *> lookup(d)

        r.run map (_ must beNone)
      }

      "should fail when nothing mounted at path" >>* {
        val f = rootDir </> dir("nothing") </> file("there")
        unmount(f).run map (dj => maybeNotFound(dj) must beSome(f))
      }
    }
  }
}
