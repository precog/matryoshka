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

object mounted {

  /** Strips the `mountPoint` off of all input paths in `ReadFile` operations
    * and restores it on output paths.
    */
   def readFile[S[_]: Functor](mountPoint: ADir)(implicit S: ReadFileF :<: S): S ~> S =
     transformPaths.readFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `WriteFile` operations
    * and restores it on output paths.
    */
  def writeFile[S[_]: Functor](mountPoint: ADir)(implicit S: WriteFileF :<: S): S ~> S =
    transformPaths.writeFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `ManageFile` operations
    * and restores it on output paths.
    */
  def manageFile[S[_]: Functor](mountPoint: ADir)(implicit S: ManageFileF :<: S): S ~> S =
    transformPaths.manageFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint))

  /** Strips the `mountPoint` off of all input paths in `QueryFile` operations
    * and restores it on output paths.
    */
  def queryFile[S[_]: Functor](mountPoint: ADir)(implicit S: QueryFileF :<: S): S ~> S =
    transformPaths.queryFile[S](stripPrefixA(mountPoint), rebaseA(mountPoint), refl)

  def fileSystem[S[_]: Functor](
    mountPoint: ADir
  )(implicit
    S0: ReadFileF :<: S,
    S1: WriteFileF :<: S,
    S2: ManageFileF :<: S,
    S3: QueryFileF :<: S
  ): S ~> S = {
    readFile[S](mountPoint)   compose
    writeFile[S](mountPoint)  compose
    manageFile[S](mountPoint) compose
    queryFile[S](mountPoint)
  }
}
