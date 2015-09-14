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

package quasar.api

import quasar.Predef._
import java.util.{zip => jzip}

import scalaz._
import scalaz.syntax.monad._
import scalaz.stream._
import scodec.bits.{ByteVector}

import quasar.fs.{Path}

object Zip {
  def zipFiles[F[_]: Monad](files: List[(Path, Process[F, ByteVector])]):
      Process[F, ByteVector] = {
    // First construct a single Process of Ops which can be performed in
    // sequence to produce the entire archive.
    sealed trait Op
    object Op {
      final case object Start extends Op
      final case class StartEntry(entry: jzip.ZipEntry) extends Op
      final case class Chunk(bytes: ByteVector) extends Op
      final case object EndEntry extends Op
      final case object End extends Op
    }

    val ops: Process[F, Op] = {
      def fileOps(path: Path, bytes: Process[F, ByteVector]) =
        Process.emit(Op.StartEntry(new jzip.ZipEntry(path.simplePathname))) ++
      bytes.map(Op.Chunk(_)) ++
      Process.emit(Op.EndEntry)

      Process.emit(Op.Start) ++
      Process.emitAll(files).flatMap((fileOps _).tupled) ++
      Process.emit(Op.End)
    }

    // Wrap up ZipOutputStream's statefulness in a class offering just two
    // mutating operations: one to accept an Op to be processed, and another
    // to poll for data that's been written.
    class Buffer {
      private var chunks = ByteVector.empty

      private def append(bytes: ByteVector) = chunks = chunks ++ bytes

      private val sink = {
        val os = new java.io.OutputStream {
          def write(b: Int) = append(ByteVector(b.toByte))

          // NB: overriding here to process each buffer-worth coming from the ZipOS in one call
          override def write(b: Array[Byte], off: Int, len: Int) = append(ByteVector(b, off, len))
        }
        new jzip.ZipOutputStream(os)
      }

      def accept(op: Op): F[Unit] = (op match {
        case Op.Start             => ()
        case Op.StartEntry(entry) => sink.putNextEntry(entry)
        case Op.Chunk(bytes)      => sink.write(bytes.toArray)
        case Op.EndEntry          => sink.closeEntry
        case Op.End               => sink.close
      }).point[F]

      def poll: F[ByteVector] = {
        val result = chunks
        chunks = ByteVector.empty
        result
      }.point[F]
    }

    // Fold the allocation of Buffer instances in to the processing
    // of Ops, so that a new instance is created as needed each time
    // the resulting process is run, then flatMap so that each chunk
    // can be handled in Task.
    ops.zipWithState[Option[Buffer]](None) {
      case (_, None)         => Some(new Buffer)
      case (Op.End, Some(_)) => None
      case (_, buf@Some(_))  => buf
    }.flatMap {
      case (Op.Start, _)   => Process.emit(ByteVector.empty)
      case (op, Some(buf)) =>
        Process.await(for {
          _ <- buf.accept(op)
          b <- buf.poll
        } yield b) { bytes =>
          if (bytes.size == 0) Process.halt
          else Process.emit(bytes)
        }
      case (_, None)       => Process.fail(new RuntimeException("unexpected state"))
    }
  }
}
