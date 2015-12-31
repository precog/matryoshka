/*
 * Copyright 2014 - 2015 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.fs

import quasar.Predef.{Vector, Unit}
import quasar.Data

import scalaz._

/** Typeclass representing the interface to a effectful cursor of `Data`.
  *
  * Laws
  *   1. close(c) *> nextChunk(c) must return an empty `Vector`.
  */
trait DataCursor[F[_], C] {
  /** Returns the next chunk of data from the cursor. An empty `Vector` signals
    * no more data is available.
    */
  def nextChunk(cursor: C): F[Vector[Data]]

  /** Closes the cursor, freeing any resources it might be using. */
  def close(cursor: C): F[Unit]
}

object DataCursor {
  def apply[F[_], C](implicit DC: DataCursor[F, C]): DataCursor[F, C] = DC

  implicit def eitherDataCursor[F[_], A, B](
    implicit A: DataCursor[F, A], B: DataCursor[F, B]
  ): DataCursor[F, A \/ B] =
    new DataCursor[F, A \/ B] {
      def nextChunk(ab: A \/ B) =
        ab fold (A.nextChunk, B.nextChunk)

      def close(ab: A \/ B) =
        ab fold (A.close, B.close)
    }
}
