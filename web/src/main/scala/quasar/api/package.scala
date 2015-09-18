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

import quasar.Predef._
import quasar.fs._

import argonaut.{DecodeResult => _, _}
import Argonaut._

import org.http4s._
import org.http4s.argonaut._
import org.http4s.dsl.{Path => HPath, listInstance => _, _}
import org.http4s.headers._
import org.http4s.server._
import org.http4s.util._

import scalaz._, Scalaz._
import scalaz.concurrent._

package object api {
  // Note: CORS middleware is comming in http4s post-0.6.5
  def `Access-Control-Allow-Origin`(value: String) = Header("Access-Control-Allow-Origin", value)
  val AccessControlAllowOriginAll = `Access-Control-Allow-Origin`("*")

  def `Access-Control-Allow-Methods`(methods: List[String]) = Header("Access-Control-Allow-Methods", methods.mkString(", "))
  def `Access-Control-Allow-Headers`(headers: List[HeaderKey]) = Header("Access-Control-Allow-Headers", headers.map(_.name).mkString(", "))
  def `Access-Control-Max-Age`(seconds: Long) = Header("Access-Control-Max-Age", seconds.toString)

  val versionAndNameInfo = jObjectAssocList(List("version" -> jString(quasar.build.BuildInfo.version), "name" -> jString("Quasar")))

  object Destination extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("Destination")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name == name) Some(header)
      else None
    }
  }

  object FileName extends HeaderKey.Singleton {
    type HeaderT = Header
    val name = CaseInsensitiveString("X-File-Name")
    override def matchHeader(header: Header): Option[HeaderT] = {
      if (header.name == name) Some(header)
      else None
    }
  }

  object Cors extends Middleware {
    // Note: CORS middleware is coming in http4s post-0.6.5
    val corsHeaders = List(
      AccessControlAllowOriginAll,
      `Access-Control-Allow-Methods`(List("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      `Access-Control-Max-Age`(20*24*60*60),
      `Access-Control-Allow-Headers`(List(Destination)))  // NB: actually needed for POST only

    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        service(req).flatMap {
          case None if req.method == OPTIONS => Ok().map(Some(_))
          case r => Task.now(r)
        }.map(_.map(_.putHeaders(corsHeaders: _*)))
      }
  }

  /** Handle failure in Task by returning a 500. Otherwise http4s hangs for 30 seconds and then returns 200. */
  object FailSafe extends Middleware {
    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        service.run(req).handleWith {
          case err => InternalServerError(Json("error" := ("uncaught: " + err.toString))).map(Some(_))
        }
      }
  }

  object HeaderParam extends Middleware {
    type HeaderValues = Map[CaseInsensitiveString, List[String]]

    def parse(param: String): String \/ HeaderValues = {
      def strings(json: Json): String \/ List[String] =
        json.string.map(str => \/-(str :: Nil)).getOrElse(
          json.array.map { vs =>
            vs.map(v => v.string \/> ("expected string in array; found: " + v.toString)).sequenceU
          }.getOrElse(-\/("expected a string or array of strings; found: " + json)))

      for {
        json <- Parse.parse(param).leftMap("parse error (" + _ + ")")
        obj <- json.obj \/> ("expected a JSON object; found: " + json.toString)
        values <- obj.toList.map { case (k, v) =>
          strings(v).map(CaseInsensitiveString(k) -> _)
        }.sequenceU
      } yield Map(values: _*)
    }

    def rewrite(headers: Headers, param: HeaderValues): Headers =
      Headers(
        param.toList.flatMap {
          case (k, vs) => vs.map(v => Header.Raw(CaseInsensitiveString(k), v))
        } ++
        headers.toList.filterNot(h => param contains h.name))

    def apply(service: HttpService): HttpService =
      Service.lift { req =>
        (req.params.get("request-headers").fold[String \/ Request](\/-(req)) { v =>
          parse(v).map(hv => req.copy(headers = rewrite(req.headers, hv)))
        }).fold(
          err => BadRequest(Json("error" := "invalid request-headers: " + err)).map(Some(_)),
          service.run)
      }
  }

  object Prefix {
    def apply(prefix: String)(service: HttpService): HttpService = {
      import monocle.macros.GenLens
      import scalaz.std.option._

      val _uri_path = GenLens[Request](_.uri) composeLens GenLens[Uri](_.path)

      val stripChars = prefix match {
        case "/"                    => 0
        case x if x.startsWith("/") => x.length
        case x                      => x.length + 1
      }

      def rewrite(path: String): Option[String] =
        if (path.startsWith(prefix)) Some(path.substring(stripChars))
        else None

      Service.lift { req: Request =>
        _uri_path.modifyF(rewrite)(req) match {
          case Some(req1) => service(req1)
          case None       => Task.now(None)
        }
      }
    }
  }

  object AsPath {
    def unapply(p: HPath): Option[Path] = {
      Some(Path("/" + p.toList.map(java.net.URLDecoder.decode(_, "UTF-8")).mkString("/")))
    }
  }

  object AsDirPath {
    def unapply(p: HPath): Option[Path] = AsPath.unapply(p).map(_.asDir)
  }

  def staticFileService(basePath: String) = HttpService {
    case GET -> AsPath(path) =>
      // NB: http4s/http4s#265 should give us a simple way to handle this stuff.
      val filePath = basePath + path.toString
      StaticFile.fromString(filePath).fold(
        StaticFile.fromString(filePath + "/index.html").fold(
          NotFound("Couldn’t find page " + path.toString))(
          Task.now))(
        resp => path.file.flatMap(f => fileMediaType(f.value)).fold(
          Task.now(resp))(
          mt => Task.delay(resp.withContentType(Some(`Content-Type`(mt))))))
  }

  def fileMediaType(file: String): Option[MediaType] =
    MediaType.forExtension(file.split('.').last)

  def redirectService(basePath: String) = HttpService {
    // NB: this means we redirected to a path that wasn't handled, and need
    // to avoid getting into a loop.
    case GET -> path if path.startsWith(HPath(basePath)) => NotFound()

    case GET -> AsPath(path) =>
      TemporaryRedirect(Uri(path = basePath + path.toString))
  }
}
