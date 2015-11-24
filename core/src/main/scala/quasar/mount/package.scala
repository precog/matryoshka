package quasar

import scalaz.{Coyoneda, EitherT}

package object mount {
  type MountingF[A] = Coyoneda[Mounting, A]
  type MntErrT[F[_], A] = EitherT[F, MountingError, A]
}
