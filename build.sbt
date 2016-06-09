import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.license.Apache2_0
import scoverage._
import sbt._
import Keys._
import org.scalajs.sbtplugin.cross.CrossProject

lazy val checkHeaders = taskKey[Unit]("Fail the build if createHeaders is not up-to-date")

val monocleVersion = "1.2.1"
val scalazVersion = "7.2.1"
// Latest version built against scalacheck 1.12.5
// doesn't support scala.js yet, until then tests are JVM-only
val specs2Version = "3.7"

val testDependencies = libraryDependencies ++= Seq(
  "org.typelevel"  %% "discipline"                % "0.4"          % "test",
  "com.github.julien-truffaut" %% "monocle-law"   % monocleVersion % "test",
  "org.scalaz"     %% "scalaz-scalacheck-binding" % scalazVersion  % "test",
  "org.typelevel"  %% "scalaz-specs2"             % "0.4.0"        % "test",
  "org.specs2"     %% "specs2-core"               % specs2Version  % "test" force(),
  "org.specs2"     %% "specs2-scalacheck"         % specs2Version  % "test" force(),
  // `scalaz-scalack-binding` is built with `scalacheck` 1.12.5 so we are stuck
  // with that version
  "org.scalacheck" %% "scalacheck"                % "1.12.5"       % "test" force()
)

lazy val standardSettings = Seq(
  headers := Map(
    "scala" -> Apache2_0("2014–2016", "SlamData Inc."),
    "java"  -> Apache2_0("2014–2016", "SlamData Inc.")),
  scalaVersion := "2.11.8",
  logBuffered in Compile := false,
  logBuffered in Test := false,
  outputStrategy := Some(StdoutOutput),
  updateOptions := updateOptions.value.withCachedResolution(true),
  autoCompilerPlugins := true,
  autoAPIMappings := true,
  exportJars := true,
  organization := "com.slamdata",
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "bintray/non" at "http://dl.bintray.com/non/maven"),
  addCompilerPlugin("org.spire-math" %% "kind-projector"   % "0.7.1"),
  addCompilerPlugin("org.scalamacros" % "paradise"         % "2.1.0" cross CrossVersion.full),
  addCompilerPlugin("com.milessabin"  % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full),
  ScoverageKeys.coverageHighlighting := true,

  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    // "-Xlint",
    "-Yno-adapted-args",
    "-Yno-imports",
    "-Ywarn-dead-code", // N.B. doesn't work well with the ??? hole
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"),
  scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import"),
  wartremoverErrors in (Compile, compile) ++= warts, // Warts.all,

  console <<= console in Test, // console alias test:console

  libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %%% "monocle-core" % monocleVersion % "compile, test",
    "org.scalaz"                 %%% "scalaz-core"  % scalazVersion  % "compile, test",
    "com.github.mpilquist"       %%% "simulacrum"   % "0.7.0"        % "compile, test"),

  licenses += ("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0")),

  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty) error("headers not all present")
  })

// Using a Seq of desired warts instead of Warts.allBut due to an incremental compilation issue.
// https://github.com/puffnfresh/wartremover/issues/202
// omissions:
//   Wart.Any
//   Wart.ExplicitImplicitTypes - see mpilquist/simulacrum#35
//   Wart.Nothing
//   Wart.Throw
val warts = Seq(
  Wart.Any2StringAdd,
  Wart.AsInstanceOf,
  Wart.DefaultArguments,
  Wart.EitherProjectionPartial,
  Wart.Enumeration,
  Wart.FinalCaseClass,
  Wart.IsInstanceOf,
  Wart.JavaConversions,
  Wart.ListOps,
  Wart.MutableDataStructures,
  Wart.NoNeedForMonad,
  Wart.NonUnitStatements,
  Wart.Null,
  Wart.Option2Iterable,
  Wart.OptionPartial,
  Wart.Product,
  Wart.Serializable,
  Wart.Return,
  Wart.ToString,
  Wart.TryPartial,
  Wart.Var)

lazy val publishSettings = Seq(
  organizationName := "SlamData Inc.",
  organizationHomepage := Some(url("http://slamdata.com")),
  homepage := Some(url("https://github.com/slamdata/matryoshka")),
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  autoAPIMappings := true,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/slamdata/matryoshka"),
      "scm:git@github.com:slamdata/matryoshka.git")),
  developers := List(
    Developer(
      id = "slamdata",
      name = "SlamData Inc.",
      email = "contact@slamdata.com",
      url = new URL("http://slamdata.com"))))

lazy val root = Project("root", file("."))
  .settings(standardSettings ++ publishSettings: _*)
  .settings(Seq(
    publish := (),
    publishLocal := (),
    publishArtifact := false))
  .settings(name := "matryoshka")
  .aggregate(coreJVM, coreJS)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = crossProject.in(file("core"))
  .settings(standardSettings ++ publishSettings: _*)
  .settings(name := "matryoshka-core")
  .jvmSettings(testDependencies)
  .enablePlugins(AutomateHeaderPlugin)

lazy val coreJVM = core.jvm
lazy val coreJS = core.js
