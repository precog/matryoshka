import sbt._
import Keys._
import CustomKeys._
import de.heikoseeberger.sbtheader.license.Apache2_0
import de.heikoseeberger.sbtheader.HeaderPlugin
import scoverage._

lazy val checkHeaders = taskKey[Unit]("Fail the build if createHeaders is not up-to-date")

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
        >= 7,
      "Java 7 or above required")
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
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import",
    "-Ywarn-value-discard"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import"
  ),
  wartremoverErrors in (Compile, compile) ++= warts, // Warts.all,

  console <<= console in Test, // console alias test:console

  scalazVersion := "7.1.4",
  slcVersion    := "0.4",

  libraryDependencies ++= Seq(
    "org.scalaz"        %% "scalaz-core"               % scalazVersion.value % "compile, test",
    "org.typelevel"     %% "shapeless-scalaz"          % slcVersion.value    % "compile, test",
    "com.github.mpilquist" %% "simulacrum"             % "0.4.0"             % "compile, test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion.value % "test",
    "org.specs2"        %% "specs2-core"               % "2.4"               % "test",
    "org.scalacheck"    %% "scalacheck"                % "1.11.6"            % "test" force(),
    "org.typelevel"     %% "scalaz-specs2"             % "0.3.0"             % "test",
    "org.typelevel"     %% "shapeless-scalacheck"      % slcVersion.value    % "test"),

  licenses += ("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0")),

  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty) error("headers not all present")
  }
)

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

lazy val root = Project("root", file(".")) aggregate(core) enablePlugins(AutomateHeaderPlugin)

lazy val core = (project in file("core")) settings (oneJarSettings: _*) enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)
