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

package quasar.config

import quasar.Predef._
import quasar.fp._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import Leibniz.===
import pathy._, Path._

sealed trait FsPath[T, S] {
  type Base

  def fold[X](uni: Path[Base, T, S] => X, inv: (String, Path[Base, T, S], Base === Abs) => X): X

  def forgetBase: FsPath[T, S] = this

  def path: Path[Base, T, S] =
    fold(ι, (_, p, _) => p)
}

object FsPath {
  type Aux[Base0, T, S] = FsPath[T, S] { type Base = Base0 }

  private final case class Uni[B, T, S](p: Path[B, T, S]) extends FsPath[T, S] {
    type Base = B
    def fold[X](uni: Path[Base, T, S] => X, inv: (String, Path[Base, T, S], Base === Abs) => X): X =
      uni(p)
  }

  private final case class InV[B, T, S](vol: String, p: Path[B, T, S], ev: B === Abs) extends FsPath[T, S] {
    type Base = B
    def fold[X](uni: Path[Base, T, S] => X, inv: (String, Path[Base, T, S], Base === Abs) => X): X =
      inv(vol, p, ev)
  }

  object Uniform {
    def apply[B, T, S](path: Path[B, T, S]): Aux[B, T, S] =
      Uni[B, T, S](path)
  }

  object InVolume {
    def apply[B, T, S](vol: String, path: Path[B, T, S])(implicit ev: B === Abs): Aux[B, T, S] =
      InV[B, T, S](vol, path, ev)
  }

  def codecForOS(os: OS): PathCodec =
    if (os.isWin) windowsCodec else posixCodec

  val systemCodec: Task[PathCodec] =
    OS.currentOS map codecForOS

  def printFsPath[T](codec: PathCodec, fp: FsPath[T, Sandboxed]): String =
    fp.fold(codec.printPath, (v, p, _) => v + codec.printPath(p))

  def sandboxFsPathIn[B, T, S](dir: Path[B, Dir, Sandboxed], fp: Aux[B, T, S]): Option[Aux[B, T, Sandboxed]] =
    fp.fold(
      p          => sandbox(dir, p) map (p1 => Uniform(dir </> p1)),
      (v, p, ev) => sandbox(dir, p) map (p1 => InVolume(v, dir </> p1)(ev))
    )

  private def winVolAndPath(s: String): (String, String) = {
    val (prefix, s1): (String, String) = if (s.startsWith("\\\\")) ("\\\\", s.drop(2)) else ("", s)
    val (vol, rest) = s1.splitAt(s1.indexOf("\\"))
    (prefix + vol, rest)
  }

  def parseWinAbsDir(s: String): Option[Aux[Abs, Dir, Sandboxed]] = {
    val (vol, rest) = winVolAndPath(s)
    windowsCodec.parseAbsDir(rest) >>= (p => sandboxFsPathIn(rootDir, InVolume(vol, p)))
  }

  def parseWinAbsFile(s: String): Option[Aux[Abs, File, Sandboxed]] = {
    val (vol, rest) = winVolAndPath(s)
    windowsCodec.parseAbsFile(rest) >>= (p => sandboxFsPathIn(rootDir, InVolume(vol, p)))
  }

  def parseWinAbsAsDir(s: String): Option[Aux[Abs, Dir, Sandboxed]] = {
    val (vol, rest) = winVolAndPath(s)
    windowsCodec.parseAbsAsDir(rest) >>= (p => sandboxFsPathIn(rootDir, InVolume(vol, p)))
  }

  private def forCurrentOS[A](f: OS => Option[A]): OptionT[Task, A] =
    OptionT(OS.currentOS map f)

  def parseAbsDir(os: OS, s: String): Option[Aux[Abs, Dir, Sandboxed]] = {
    def parseOther(codec: PathCodec) =
      codec.parseAbsDir(s) >>= (p => sandboxFsPathIn(rootDir, Uniform(p)))

    if (os.isWin) parseWinAbsDir(s) else parseOther(codecForOS(os))
  }

  def parseSystemAbsDir(s: String): OptionT[Task, Aux[Abs, Dir, Sandboxed]] =
    forCurrentOS(parseAbsDir(_, s))

  def parseAbsFile(os: OS, s: String): Option[Aux[Abs, File, Sandboxed]] = {
    def parseOther(codec: PathCodec) =
      codec.parseAbsFile(s) >>= (p => sandboxFsPathIn(rootDir, Uniform(p)))

    if (os.isWin) parseWinAbsFile(s) else parseOther(codecForOS(os))
  }

  def parseAbsAsDir(os: OS, s: String): Option[Aux[Abs, Dir, Sandboxed]] = {
    def parseOther(codec: PathCodec) =
      codec.parseAbsAsDir(s) >>= (p => sandboxFsPathIn(rootDir, Uniform(p)))

    if (os.isWin) parseWinAbsAsDir(s) else parseOther(codecForOS(os))
  }

  def parseSystemAbsFile(s: String): OptionT[Task, Aux[Abs, File, Sandboxed]] =
    forCurrentOS(parseAbsFile(_, s))

  def parseRelFile(os: OS, s: String): Option[Aux[Rel, File, Sandboxed]] =
    codecForOS(os).parseRelFile(s) >>= (p => sandboxFsPathIn(currentDir, Uniform(p)))

  def parseSystemRelFile(s: String): OptionT[Task, Aux[Rel, File, Sandboxed]] =
    forCurrentOS(parseRelFile(_, s))

  def parseFile(os: OS, s: String): Option[FsPath[File, Sandboxed]] =
    parseAbsFile(os, s).map(_.forgetBase) orElse parseRelFile(os, s).map(_.forgetBase)

  def parseSystemFile(s: String): OptionT[Task, FsPath[File, Sandboxed]] =
    forCurrentOS(parseFile(_, s))

  implicit class FsDirOps[B, S](fd: Aux[B, Dir, S]) {
    def </>[T](rel: Path[Rel, T, S]): Aux[B, T, S] =
      fd.fold(
        p          => Uniform(p </> rel),
        (v, p, ev) => InVolume(v, p </> rel)(ev)
      )
  }
}
