import sbt._
import Keys._

val scalazVersion     = "7.1.0"
val monocleVersion    = "0.5.0"
val unfilteredVersion = "0.8.1"

lazy val standardSettings = Defaults.defaultSettings ++ Seq(
  scalaVersion := "2.11.4",
  logBuffered in Compile := false,
  logBuffered in Test := false,
  outputStrategy := Some(StdoutOutput),
  initialize := {
    assert(
      Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
        >= 7,
      "Java 7 or above required")
  },
  autoCompilerPlugins := true,
  exportJars := true,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "bintray/non" at "http://dl.bintray.com/non/maven"
  ),
  // TODO: use this to replace type lambdas once non/kind-projector#7 is fixed.
  // addCompilerPlugin("org.spire-math" % "kind-projector_2.11" % "0.5.2"),
  scalacOptions ++= Seq(
    "-Xfatal-warnings",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:existentials",
    "-language:postfixOps"
  ),
  libraryDependencies ++= Seq(
    "org.scalaz"        %% "scalaz-core"               % scalazVersion     % "compile, test",
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion     % "compile, test",
    "org.scalaz.stream" %% "scalaz-stream"             % "0.5a"            % "compile, test",
    "org.spire-math"    %% "spire"                     % "0.8.2"           % "compile, test",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion    % "compile, test",
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion    % "compile, test",
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion    % "compile, test",
    "org.threeten"      %  "threetenbp"                % "1.2"             % "compile, test",
    "org.mongodb"       %  "mongo-java-driver"         % "2.12.2"          % "compile, test",
    "net.databinder"    %% "unfiltered-filter"         % unfilteredVersion % "compile, test",
    "net.databinder"    %% "unfiltered-netty-server"   % unfilteredVersion % "compile, test",
    "net.databinder"    %% "unfiltered-netty"          % unfilteredVersion % "compile, test",
    "io.argonaut"       %% "argonaut"                  % "6.1-M5"          % "compile, test",
    "org.jboss.aesh"    %  "aesh"                      % "0.55"            % "compile, test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion     % "compile, test",
    "com.github.julien-truffaut" %% "monocle-law"      % monocleVersion    % "compile, test",
    "org.specs2"        %% "specs2-core"       % "2.3.13-scalaz-7.1.0-RC1" % "test",
    "org.typelevel"     %% "scalaz-specs2"             % "0.3.0"           % "test",
    "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"          % "test"
  ),
  licenses += ("GNU Affero GPL V3", url("http://www.gnu.org/licenses/agpl-3.0.html")))

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
  GithubKeys.repoSlug := "slamdata/slamengine",

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

lazy val root = Project("root", file(".")) aggregate(core, web, admin, it)

lazy val core = (project in file("core")) settings (oneJarSettings: _*)

lazy val web = (project in file("web")) dependsOn (core % "test->test;compile->compile") settings (oneJarSettings: _*)

lazy val it = (project in file("it")) dependsOn (core % "test->test;compile->compile", web) settings (standardSettings: _*)

lazy val admin = (project in file("admin")) dependsOn (core) settings (oneJarSettings: _*)
