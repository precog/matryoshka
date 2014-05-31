package slamdata.engine

import scalaz.concurrent._
import scalaz.stream._

import scodec.bits.ByteVector

import unfiltered.response._

package object api {
  def ResponseProcess[A](p: Process[Task, A])(f: A => ByteVector): ResponseStreamer = new ResponseStreamer {
    def stream(os: java.io.OutputStream): Unit = {
      val rez = p.map(f) to io.chunkW(os)

      rez.run.run

      ()
    }
  }
}