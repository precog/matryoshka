//: ----------------------------------------------------------------------------
//: Copyright (C) 2014 Verizon.  All Rights Reserved.
//:
//:   Licensed under the Apache License, Version 2.0 (the "License");
//:   you may not use this file except in compliance with the License.
//:   You may obtain a copy of the License at
//:
//:       http://www.apache.org/licenses/LICENSE-2.0
//:
//:   Unless required by applicable law or agreed to in writing, software
//:   distributed under the License is distributed on an "AS IS" BASIS,
//:   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//:   See the License for the specific language governing permissions and
//:   limitations under the License.
//:
//: ----------------------------------------------------------------------------

package remotely

import java.util.concurrent.atomic.AtomicReference
import scala._

import scalaz.concurrent.Task

/** An atomically updatable reference, guarded by the `Task` monad. */
sealed abstract class IORef[A] {
  def read: Task[A]
  def write(value: A): Task[Unit]
  def atomicModify[B](f: A => (A, B)): Task[B]
  def compareAndSet(oldVal: A, newVal: A): Task[Boolean]
  def modify(f: A => A): Task[Unit] =
    atomicModify(a => (f(a), ()))
}

object IORef {
  def apply[A](value: => A): IORef[A] = new IORef[A] {
    val ref = new AtomicReference(value)
    def read = Task(ref.get)
    def write(value: A) = Task(ref.set(value))
    def compareAndSet(oldVal: A, newVal: A) =
      Task(ref.compareAndSet(oldVal, newVal))
    def atomicModify[B](f: A => (A, B)) = for {
      a <- read
      (a2, b) = f(a)
      p <- compareAndSet(a, a2)
      r <- if (p) Task.now(b) else atomicModify(f)
    } yield r
  }
}
