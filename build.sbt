import sbt._
import Keys._
import de.heikoseeberger.sbtheader.license.Apache2_0
import de.heikoseeberger.sbtheader.HeaderPlugin
import ScoverageSbtPlugin._

val scalazVersion  = "7.1.3"
val slcVersion     = "0.4"
val monocleVersion = "1.1.1"

lazy val standardSettings = Defaults.defaultSettings ++ Seq(
  headers := Map(
    "scala" -> Apache2_0("2014 - 2015", "SlamData Inc."),
    "java"  -> Apache2_0("2014 - 2015", "SlamData Inc.")),
  scalaVersion := "2.11.7",
  logBuffered in Compile := false,
  logBuffered in Test := false,
  outputStrategy := Some(StdoutOutput),
  initialize := {
    assert(
      Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
        >= 8,
      "Java 8 or above required")
  },
  autoCompilerPlugins := true,
  autoAPIMappings := true,
  exportJars := true,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "bintray/non" at "http://dl.bintray.com/non/maven"),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.5.4"),
  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full),

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
    // "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import"
  ),
  console <<= console in Test, // console alias test:console
  initialCommands in (Test, console) := """ammonite.repl.Repl.run("prompt.update(\"λ \")")""",
  libraryDependencies ++= Seq(
    "com.lihaoyi"        % "ammonite-repl"             % "0.4.7"        % "test" cross CrossVersion.full,
    "org.scalaz"        %% "scalaz-core"               % scalazVersion  % "compile, test",
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion  % "compile, test",
    "org.scalaz.stream" %% "scalaz-stream"             % "0.7.3a"       % "compile, test",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion % "compile, test",
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion % "compile, test",
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion % "compile, test",
    "com.github.scopt"  %% "scopt"                     % "3.3.0"        % "compile, test",
    "org.threeten"      %  "threetenbp"                % "1.2"          % "compile, test",
    "org.mongodb"       %  "mongo-java-driver"         % "3.0.2"        % "compile, test",
    "io.argonaut"       %% "argonaut"                  % "6.1"          % "compile, test",
    "org.jboss.aesh"    %  "aesh"                      % "0.55"         % "compile, test",
    "org.typelevel"     %% "shapeless-scalaz"          % slcVersion     % "compile, test",
    "com.slamdata"      %% "pathy"                     % "0.0.1-SNAPSHOT" % "compile, test",
    "com.github.mpilquist" %% "simulacrum"             % "0.3.0"        % "compile, test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion  % "test",
    "org.specs2"        %% "specs2-core"               % "2.4"          % "test",
    "org.typelevel"     %% "scalaz-specs2"             % "0.3.0"        % "test",
    "org.typelevel"     %% "shapeless-scalacheck"      % slcVersion     % "test",
    "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"       % "test"),
  licenses += ("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0")))

import github.GithubPlugin._

lazy val oneJarSettings = {
  import sbtrelease.ReleasePlugin._
  import sbtrelease.ReleaseStateTransformations._
  import sbtrelease._

  import sbt._
  import sbt.Aggregation.KeyValue
  import sbt.std.Transform.DummyTaskMap
  import Utilities._

  def releaseHack[T](key: TaskKey[T]): ReleaseStep = { st: State =>
    val extracted = st.extract
    val ref = extracted.get(thisProjectRef)
    extracted.runTask(key in ref, st)
    st
  }

  com.github.retronym.SbtOneJar.oneJarSettings ++ standardSettings ++ githubSettings ++ releaseSettings ++ Seq(
  GithubKeys.assets := { Seq(oneJar.value) },
  GithubKeys.repoSlug := "quasar-analytics/quasar",

  GithubKeys.versionRepo := "slamdata/slamdata.github.io",
  GithubKeys.versionFile := "release.json",

  ReleaseKeys.versionFile := file("version.sbt"),
  ReleaseKeys.useGlobalVersion := true,
  ReleaseKeys.commitMessage <<= (version in ThisBuild) map { v =>
    if (v.matches(""".*SNAPSHOT.*""")) ("Setting version to %s" format v) + " [ci skip]"
    else "Releasing %s" format v
  },
  ReleaseKeys.releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,              // : ReleaseStep
    inquireVersions,                        // : ReleaseStep
    runTest,                                // : ReleaseStep
    setReleaseVersion,                      // : ReleaseStep
    commitReleaseVersion,                   // : ReleaseStep, performs the initial git checks
    pushChanges,                            // : ReleaseStep
    // releaseHack(GithubKeys.githubUpdateVer),// : Don't update git version, have the installers task do that
    // tagRelease,                             // : Don't tag release because Travis will do it
    // releaseHack(GithubKeys.githubRelease),  // : Don't release because Travis will do it
    setNextVersion,                         // : ReleaseStep
    commitNextVersion,                      // : ReleaseStep
    pushChanges                             // : ReleaseStep, also checks that an upstream branch is properly configured
  ))
}

lazy val root = Project("root", file(".")) aggregate(core, web, it) enablePlugins(AutomateHeaderPlugin)

lazy val core = (project in file("core")) settings (oneJarSettings: _*) enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

lazy val web = (project in file("web")) dependsOn (core % "test->test;compile->compile") settings (oneJarSettings: _*) enablePlugins(AutomateHeaderPlugin)

lazy val it = (project in file("it")) dependsOn (core % "test->test;compile->compile", web % "test->test;compile->compile") settings (standardSettings: _*) enablePlugins(AutomateHeaderPlugin)
