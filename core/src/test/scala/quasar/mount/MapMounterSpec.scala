package quasar.mount

import quasar.Predef._
import quasar.EnvironmentError2
import quasar.fp._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MapMounterSpec extends MountingSpec[MountingF] {
  import MapMounter.{Mounts, MountR}, MountConfig2._

  val invalidUri = ConnectionUri(uriA.value + "INVALID")
  val unsuppUri  = ConnectionUri(uriB.value + "VERSION")
  val unsuppErr  = EnvironmentError2.unsupportedVersion("DB", List(1,2,3))

  val doMount: MountConfig2 => MountR = {
    case FileSystemConfig(`dbType`, `invalidUri`) => Either3.left3("invalid URI")
    case FileSystemConfig(`dbType`, `unsuppUri`)  => Either3.middle3(unsuppErr)
    case _                                        => Either3.right3(())
  }

  def interpName = "MapMounter"

  def interpret = {
    val mm = MapMounter[Id](doMount)
    val ref = TaskRef(Mounts.empty).run

    val interp0: Mounting ~> Task = new (Mounting ~> Task) {
      def apply[A](m: Mounting[A]) = ref.modifyS(mm(m).run)
    }

    Coyoneda.liftTF(interp0)
  }

  "Handling mounts" should {
    "result in an invalid config when mount returns a string" >>* {
      val loc = rootDir </> dir("fs")
      val cfg = MountConfig2.fileSystemConfig(dbType, invalidUri)

      mnt.mountFileSystem(loc, dbType, invalidUri)
        .run.tuple(mnt.lookup(loc).run)
        .map(_ must_== ((MountingError.invalidConfig(cfg, "invalid URI").left, None)))
    }

    "result in an environment error when mount returns an error" >>* {
      val loc = rootDir </> dir("fs2")
      val cfg = MountConfig2.fileSystemConfig(dbType, unsuppUri)

      mnt.mountFileSystem(loc, dbType, unsuppUri)
        .run.tuple(mnt.lookup(loc).run)
        .map(_ must_== ((MountingError.environmentError(unsuppErr).left, None)))
    }
  }
}
