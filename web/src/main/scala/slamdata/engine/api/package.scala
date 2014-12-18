package slamdata.engine

import scalaz.concurrent._
import scalaz.stream._

import scodec.bits.ByteVector

import argonaut._
import Argonaut._

import unfiltered.request._
import unfiltered.response._

package object api {
  def ResponseProcess[A](p: Process[Task, A])(f: A => ByteVector): ResponseStreamer = new ResponseStreamer {
    def stream(os: java.io.OutputStream): Unit = {
      val rez = p.map(f) to io.chunkW(os)

      rez.run.run

      ()
    }
  }

  def ResponseJson(json: Json): ResponseStreamer = new ResponseStreamer {
    def stream(os: java.io.OutputStream): Unit = {
      new java.io.DataOutputStream(os).write(json.toString.getBytes("UTF-8"))

      ()
    }
  }

  object AccessControlAllowOrigin extends HeaderName("Access-Control-Allow-Origin")

  val AccessControlAllowOriginAll = AccessControlAllowOrigin("*")

  object AccessControlAllowMethods extends HeaderName("Access-Control-Allow-Methods")
  object AccessControlAllowHeaders extends HeaderName("Access-Control-Allow-Headers")
  object AccessControlMaxAge extends HeaderName("Access-Control-Max-Age")

  object DestinationReq extends UriHeader("Destination")
}