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

import scalaz._, NaturalTransformation.refl

object chroot {

  /** Rebases all paths in `ReadFile` operations onto the given prefix. */
  def readFile[S[_]: Functor](prefix: ADir)(implicit S: ReadFileF :<: S): S ~> S =
    transformPaths.readFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases all paths in `WriteFile` operations onto the given prefix. */
  def writeFile[S[_]: Functor](prefix: ADir)(implicit S: WriteFileF :<: S): S ~> S =
    transformPaths.writeFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases all paths in `ManageFile` operations onto the given prefix. */
  def manageFile[S[_]: Functor](prefix: ADir)(implicit S: ManageFileF :<: S): S ~> S =
    transformPaths.manageFile[S](rebaseA(prefix), stripPrefixA(prefix))

  /** Rebases paths in `QueryFile` onto the given prefix. */
  def queryFile[S[_]: Functor](prefix: ADir)(implicit S: QueryFileF :<: S): S ~> S =
    transformPaths.queryFile[S](rebaseA(prefix), stripPrefixA(prefix), refl)

  /** Rebases all paths in `FileSystem` operations onto the given prefix. */
  def fileSystem[S[_]: Functor](
    prefix: ADir
  )(implicit
    S0: ReadFileF :<: S,
    S1: WriteFileF :<: S,
    S2: ManageFileF :<: S,
    S3: QueryFileF :<: S
  ): S ~> S = {
    readFile[S](prefix)   compose
    writeFile[S](prefix)  compose
    manageFile[S](prefix) compose
    queryFile[S](prefix)
  }
}
