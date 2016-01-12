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

package quasar.fs.mount

import quasar.Predef._
import quasar.fs.ADir

import pathy.Path._
import scalaz._, Scalaz._

/** A mapping of values to directory paths, maintains the invariant that no
  * path is a prefix of any other path.
  *
  * The current implementation is linear in the number of mounts, might be able
  * to do better using a bintree given an `Order` on `ADir`.
  */
final class Mounts[A] private (val toMap: Map[ADir, A]) {
  def add(at: ADir, a: A): String \/ Mounts[A] = {
    def added: Mounts[A] =
      new Mounts(toMap + (at -> a))

    if (toMap contains at)
      added.right
    else
      toMap.keys.toStream.traverseU_ { mnt =>
        mnt.relativeTo(at)
          .as("existing mount below: " + posixCodec.printPath(mnt))
          .toLeftDisjunction(()) *>
        at.relativeTo(mnt)
          .as("existing mount above: " + posixCodec.printPath(mnt))
          .toLeftDisjunction(())
      } as added
  }

  def + (mount: (ADir, A)): String \/ Mounts[A] =
    add(mount._1, mount._2)

  def remove(at: ADir): Mounts[A] =
    new Mounts(toMap - at)

  def - (at: ADir): Mounts[A] =
    remove(at)

  def lookup(at: ADir): Option[A] =
    toMap.get(at)

  def mapWithDir[B](f: (ADir, A) => B): Mounts[B] =
    new Mounts(toMap map { case (d, a) => (d, f(d, a)) })

  def map[B](f: A => B): Mounts[B] =
    mapWithDir((_, a) => f(a))

  /** Right-biased union of two `Mounts`. */
  def union(other: Mounts[A]): String \/ Mounts[A] =
    other.toMap.toList.foldLeftM[String \/ ?, Mounts[A]](this)(_ + _)
}

object Mounts {
  def empty[A] = _empty.asInstanceOf[Mounts[A]]

  def singleton[A](dir: ADir, a: A): Mounts[A] =
    new Mounts(Map(dir -> a))

  def fromFoldable[F[_]: Foldable, A](entries: F[(ADir, A)]): String \/ Mounts[A] =
    entries.foldLeftM[String \/ ?, Mounts[A]](empty[A])(_ + _)

  implicit val mountsFunctor: Functor[Mounts] =
    new Functor[Mounts] {
      def map[A, B](fa: Mounts[A])(f: A => B) = fa map f
    }

  ////

  private val _empty = new Mounts(Map())
}
