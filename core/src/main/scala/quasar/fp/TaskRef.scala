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

package quasar
package fp

import quasar.Predef.{Unit, Boolean}

import java.util.concurrent.atomic.AtomicReference
import scalaz.syntax.id._
import scalaz.concurrent.Task

/** A thread-safe, atomically updatable mutable reference.
  *
  * Cribbed from the `IORef` defined in oncue/remotely, an Apache 2 licensed
  * project: https://github.com/oncue/remotely
  *
  */
sealed abstract class TaskRef[A] {
  def read: Task[A]
  def write(a: A): Task[Unit]
  def compareAndSet(oldA: A, newA: A): Task[Boolean]
  def modifyS[B](f: A => (A, B)): Task[B]
  def modify(f: A => A): Task[A] =
    modifyS(a => f(a).squared)
}

object TaskRef {
  def apply[A](initial: A): Task[TaskRef[A]] = Task delay {
    new TaskRef[A] {
      val ref = new AtomicReference(initial)
      def read = Task.delay(ref.get)
      def write(a: A) = Task.delay(ref.set(a))
      def compareAndSet(oldA: A, newA: A) =
        Task.delay(ref.compareAndSet(oldA, newA))
      def modifyS[B](f: A => (A, B)) = for {
        a0 <- read
        (a1, b) = f(a0)
        p  <- compareAndSet(a0, a1)
        b  <- if (p) Task.now(b) else modifyS(f)
      } yield b
    }
  }
}
