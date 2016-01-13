package quasar

import quasar.Predef.Map
import quasar.fs.{QueryFile, FileSystem, ADir}
import quasar.fp.free
import quasar.effect._

import scalaz.{Failure => _, _}
import scalaz.syntax.apply._
import scalaz.concurrent._

package object regression {
  import quasar.fs.mount.hierarchical.{HFSFailureF, MountedResultHF}

  type FileSystemIO[A] = Coproduct[Task, FileSystem, A]

  type HfsIO0[A] = Coproduct[MountedResultHF, Task, A]
  type HfsIO1[A] = Coproduct[HFSFailureF, HfsIO0, A]
  type HfsIO[A]  = Coproduct[MonotonicSeqF, HfsIO1, A]

  val interpretHfsIO: Task[HfsIO ~> Task] = {
    import QueryFile.ResultHandle
    import quasar.fs.mount.hierarchical._

    val handlesTask: Task[MountedResultHF ~> Task] =
      KeyValueStore.taskRefKeyValueStore[ResultHandle, (ADir, ResultHandle)](Map())
        .map(Coyoneda.liftTF[MountedResultH, Task](_))

    val monoSeqTask: Task[MonotonicSeqF ~> Task] =
      MonotonicSeq.taskRefMonotonicSeq(0)
        .map(Coyoneda.liftTF[MonotonicSeq, Task](_))

    val hfsFailTask: HFSFailureF ~> Task =
      Coyoneda.liftTF[HFSFailure, Task](
        Failure.toTaskFailure[HierarchicalFileSystemError])

    (handlesTask |@| monoSeqTask)((hndl, monos) =>
      free.interpret4(monos, hfsFailTask, hndl, NaturalTransformation.refl))
  }
}
