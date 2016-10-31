import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.license.Apache2_0
import scoverage._
import sbt._
import Keys._
import org.scalajs.sbtplugin.cross.CrossProject

lazy val checkHeaders = taskKey[Unit]("Fail the build if createHeaders is not up-to-date")

// (monocle) doesn't support scala.js yet, until then tests are JVM-only
val scalazVersion           = "7.2.7"
val specs2Version           = "3.8.5.1"
val scalacheckVersion       = "1.13.3"
val scalazScalacheckVersion = "7.2.7-scalacheck-1.13"
val disciplineVersion       = "0.7.1"

def monocleVersion(sv: String) = sv match {
  case "2.11" => "1.3.1"
  case _      => "1.4.0-SNAPSHOT"
}
def versionDeps(sv: String): Seq[ModuleID] = sv match {
  case "2.11" => Seq(compilerPlugin("com.milessabin" % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full))
  case _      => Seq()
}
def versionOpts(sv: String): Seq[String] = sv match {
  case "2.11" => Seq()
  case _      => Seq("-Ypartial-unification")
}

def universalSettings = Seq(
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq(scalaVersion.value, "2.12.0"),
  organization := "com.slamdata",
  licenses += ("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0")),
  libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %%% "monocle-core" % monocleVersion(scalaBinaryVersion.value),
    "org.scalaz"                 %%% "scalaz-core"  % scalazVersion,
    "com.github.mpilquist"       %%% "simulacrum"   % "0.10.0"
  ),
  addCompilerPlugin("org.spire-math"  %% "kind-projector"   % "0.9.3"),
  addCompilerPlugin("org.scalamacros" %  "paradise"         % "2.1.0" cross CrossVersion.full),
  libraryDependencies ++= versionDeps(scalaBinaryVersion.value),
  scalacOptions ++= Seq(
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions"
  )
)
lazy val standardSettings = universalSettings ++ Seq(
  headers := Map(
    "scala" -> Apache2_0("2014–2016", "SlamData Inc."),
    "java"  -> Apache2_0("2014–2016", "SlamData Inc.")),
  logBuffered in Compile := false,
  logBuffered in Test := false,
  outputStrategy := Some(StdoutOutput),
  updateOptions := updateOptions.value.withCachedResolution(true),
  autoCompilerPlugins := true,
  autoAPIMappings := true,
  exportJars := true,
  ScoverageKeys.coverageHighlighting := true,
  scalacOptions ++= versionOpts(scalaBinaryVersion.value),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    // "-Xlint",
    "-Yno-adapted-args",
    "-Yno-imports",
    "-Ywarn-dead-code", // N.B. doesn't work well with the ??? hole
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"
  ),
  scalacOptions in (Compile,doc) ++= Seq("-groups", "-implicits"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import",
    "-Ywarn-dead-code"
  ),
  wartremoverErrors in (Compile, compile) ++= warts, // Warts.all,
  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty) sys.error("headers not all present")
  })

// Using a Seq of desired warts instead of Warts.allBut due to an incremental compilation issue.
// https://github.com/puffnfresh/wartremover/issues/202
// omissions:
//   Wart.Any
//   Wart.ExplicitImplicitTypes - see mpilquist/simulacrum#35
//   Wart.Nothing
//   Wart.Throw
//   Wart.NoNeedForMonad - crashes with a match error after dependency version upgrades
//     scala.MatchError: (_1: A, _2: T[F])(A, T[F])((a @ _), (tf @ _)) (of class scala.reflect.internal.Trees$Apply)
//       at org.wartremover.warts.NoNeedForMonad$$anonfun$1$$anonfun$apply$1.apply(NoNeedForMonad.scala:18)
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

def noArtifacts = Seq(
          publish := (),
     publishLocal := (),
  publishArtifact := false
)
lazy val root = Project("root", file("."))
  .settings(standardSettings ++ publishSettings ++ noArtifacts: _*)
  .settings(name := "matryoshka")
  .settings(console := (console in repl).value)
  .aggregate(coreJVM, coreJS, `internal-test-glue`, repl)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = crossProject.in(file("core"))
  .settings(standardSettings ++ publishSettings: _*)
  .settings(name := "matryoshka-core")
  .enablePlugins(AutomateHeaderPlugin)

lazy val `internal-test-glue` = project settings universalSettings settings (
  libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %% "monocle-law"               % monocleVersion(scalaBinaryVersion.value),
    "org.specs2"                 %% "specs2-core"               % specs2Version,
    "org.specs2"                 %% "specs2-scalacheck"         % specs2Version,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" % scalazScalacheckVersion,
    "org.scalacheck"             %% "scalacheck"                % scalacheckVersion,
    "org.typelevel"              %% "discipline"                % disciplineVersion
  )
)

lazy val coreJVM = core.jvm dependsOn (`internal-test-glue` % "test->compile")
lazy val coreJS  = core.js

/** A project just for the console.
 *  Applies only the settings necessary for that purpose.
 */
lazy val repl = project dependsOn (coreJVM % "test->test;compile->compile") settings (universalSettings: _*) settings (
  console := (console in Test).value,
  initialCommands in console := """
    import scalaz._, Scalaz._
    import matryoshka._, data._,Recursive.ops._, FunctorT.ops._
  """
)
