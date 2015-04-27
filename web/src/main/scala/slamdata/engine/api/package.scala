package slamdata.engine

import scalaz.concurrent._
import scalaz.stream._

import org.http4s._
import org.http4s.util._

package object api {
  // Note: CORS middleware is comming in http4s post-0.6.5
  def `Access-Control-Allow-Origin`(value: String) = Header("Access-Control-Allow-Origin", value)
  val AccessControlAllowOriginAll = `Access-Control-Allow-Origin`("*")

  def `Access-Control-Allow-Methods`(methods: List[String]) = Header("Access-Control-Allow-Methods", methods.mkString(", "))
  def `Access-Control-Allow-Headers`(headers: List[HeaderKey]) = Header("Access-Control-Allow-Headers", headers.map(_.name).mkString(", "))
  def `Access-Control-Max-Age`(seconds: Long) = Header("Access-Control-Max-Age", seconds.toString)

  object Destination extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("Destination")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name == name) Some(header)
      else None
    }
  }
}
