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
