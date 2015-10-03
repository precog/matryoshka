package quasar

import quasar.fp._
import scalaz._
import pathy.Path._

package object fs {
  type ReadFileF[A]   = Coyoneda[ReadFile, A]
  type WriteFileF[A]  = Coyoneda[WriteFile, A]
  type ManageFileF[A] = Coyoneda[ManageFile, A]

  type FileSystem0[A] = Coproduct[WriteFileF, ManageFileF, A]
  type FileSystem[A]  = Coproduct[ReadFileF, FileSystem0, A]

  type RelPath[S] = RelDir[S] \/ RelFile[S]
  type AbsPath[S] = AbsDir[S] \/ AbsFile[S]

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]
  type FileSystemErrT[F[_], A] = EitherT[F, FileSystemError, A]

  def interpretFileSystem[M[_]: Functor](
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    interpret.interpret3[ReadFileF, WriteFileF, ManageFileF, M](
      Coyoneda.liftTF(r), Coyoneda.liftTF(w), Coyoneda.liftTF(m))
}

