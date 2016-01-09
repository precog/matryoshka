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

import quasar.Predef._
import quasar._
import quasar.fp._
import quasar.recursionschemes._, FunctorT.ops._

import monocle.Optional
import monocle.function.Field1
import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

/** A collection of views, each a query mapped to certain (file) path, which may
  * refer to each other and to concrete files.
  */
final case class Views(map: Map[AFile, Fix[LogicalPlan]]) {
  def contains(p: AFile): Boolean = map.contains(p)

  /** Enumerate view files and view ancestor directories at a particular location. */
  def ls(dir: ADir): Set[RDir \/ RFile] = {
    /** Extract the first node from a relative path, if any. */
    def firstNode[T, S](p: PPath[Rel, T, S]):
        Option[PPath[Rel, Dir, S] \/ PPath[Rel, File, S]] =
      PPath.flatten(
        None,
        Some(-\/(())),
        None,
        dn => Some(\/-(-\/ (PPath.dir(dn)))),
        fn => Some(\/-( \/-(PPath.file(fn)))),
        p).toList match {
          case Some(-\/(_)) :: Some(\/-(a)) :: _ => Some(a)
          case _ => None
        }

    map.keys.toList.map(
      _.relativeTo(dir).flatMap(firstNode))
      .foldMap(_.toSet)
  }

  /** Resolve a path to the query for the view found there if any. */
  def lookup(p: AFile): Option[Fix[LogicalPlan]] =
    map.get(p).map(rewrite0(_, Set(p)))

  /** Resolve view references within a query. */
  def rewrite(lp: Fix[LogicalPlan]): Fix[LogicalPlan] = rewrite0(lp, Set())

  private def rewrite0(lp: Fix[LogicalPlan], expanded: Set[AFile]): Fix[LogicalPlan] = {
    val expandedP = expanded.map(convert)
    lp.transCata(once {
      case LogicalPlan.ReadF(p) if !(expandedP contains p) =>
        convertToAFile(p).flatMap(f => map.get(f).map { lp =>
          val q = absolutize(lp, fileParent(f))
          rewrite0(q, expanded + f).unFix
        })
      case _ => None
    })
  }

  /** Rewrite relative paths to be based on the given dir. */
  private def absolutize(lp: Fix[LogicalPlan], dir: ADir): Fix[LogicalPlan] =
    lp.transCata {
      case read @ LogicalPlan.ReadF(p) =>
        p.from(convert(dir)).fold(
          κ(read),
          LogicalPlan.ReadF(_))
      case t => t
    }
}

object Views {
  def empty: Views = Views(Map.empty)
}
