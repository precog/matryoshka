import org.scalajs.sbtplugin.ScalaJSCrossVersion
import scoverage._
import sbt._
import Keys._
import slamdata.SbtSlamData.transferPublishAndTagResources

lazy val monocleVersion = "1.4.0"
lazy val scalazVersion  = "7.2.15"

lazy val standardSettings = commonBuildSettings ++ Seq(
  logBuffered in Compile := false,
  logBuffered in Test := false,
  updateOptions := updateOptions.value.withCachedResolution(true),
  exportJars := true,
  organization := "com.slamdata",
  ScoverageKeys.coverageHighlighting := true,
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-implicits"),
  wartremoverWarnings in (Compile, compile) --= Seq(
    Wart.PublicInference,    // TODO: enable incrementally — currently results in many errors
    Wart.ImplicitParameter), // see wartremover/wartremover#350 & #351

  libraryDependencies ++= Seq(
    "com.slamdata"               %%  "slamdata-predef" % "0.0.6",
    "com.github.julien-truffaut" %%% "monocle-core"    % monocleVersion % "compile, test",
    "org.scalaz"                 %%% "scalaz-core"     % scalazVersion  % "compile, test",
    "com.github.mpilquist"       %%% "simulacrum"      % "0.11.0"       % "compile, test"))

lazy val publishSettings = commonPublishSettings ++ Seq(
  organizationName := "SlamData Inc.",
  organizationHomepage := Some(url("http://slamdata.com")),
  homepage := Some(url("https://github.com/slamdata/matryoshka")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/slamdata/matryoshka"),
      "scm:git@github.com:slamdata/matryoshka.git")))

lazy val root = Project("root", file("."))
  .settings(name := "matryoshka")
  .settings(standardSettings ++ noPublishSettings: _*)
  .settings(transferPublishAndTagResources)
  .settings(console := (console in replJVM).value)
  .aggregate(
    coreJS,  scalacheckJS,  testsJS,
    coreJVM, scalacheckJVM, testsJVM,
    docs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = crossProject.in(file("core"))
  .settings(name := "matryoshka-core")
  .settings(standardSettings ++ publishSettings: _*)
  .enablePlugins(AutomateHeaderPlugin)

lazy val scalacheck = crossProject
  .dependsOn(core)
  .settings(name := "matryoshka-scalacheck")
  .settings(standardSettings ++ publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    // NB: Needs a version of Scalacheck with rickynils/scalacheck#301.
    "org.scalacheck" %% "scalacheck"                % "1.14.0-861f58e-SNAPSHOT",
    "org.scalaz"     %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.13")))
  .enablePlugins(AutomateHeaderPlugin)

lazy val tests = crossProject
  .settings(name := "matryoshka-tests")
  .dependsOn(core, scalacheck)
  .settings(standardSettings ++ noPublishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %% "monocle-law"   % monocleVersion % Test,
    "org.typelevel"              %% "scalaz-specs2" % "0.5.0"        % Test,
    "org.specs2"                 %% "specs2-core"   % "3.8.7"        % Test))
  .enablePlugins(AutomateHeaderPlugin)

lazy val docs = project
  .settings(name := "matryoshka-docs")
  .dependsOn(coreJVM)
  .settings(standardSettings ++ noPublishSettings: _*)
  .settings(tutScalacOptions --= Seq("-Yno-imports", "-Ywarn-unused-import"))
  .enablePlugins(MicrositesPlugin)
  .settings(
    micrositeName             := "Matryoshka",
    micrositeDescription      := "Generalized folds, unfolds, and traversals for fixed point data structures in Scala.",
    micrositeAuthor           := "SlamData",
    micrositeGithubOwner      := "slamdata",
    micrositeGithubRepo       := "matryoshka",
    micrositeBaseUrl          := "/matryoshka",
    micrositeDocumentationUrl := "/matryoshka/docs/01-Index.html",
    micrositeHighlightTheme   := "color-brewer")

/** A project just for the console.
  * Applies only the settings necessary for that purpose.
  */
lazy val repl = crossProject dependsOn (tests % "compile->test") settings standardSettings settings (
  console := (console in Test).value,
  scalacOptions --= Seq("-Yno-imports", "-Ywarn-unused-import"),
  initialCommands in console += """
    |import matryoshka._
    |import matryoshka.data._
    |import matryoshka.implicits._
    |import matryoshka.patterns._
    |import scalaz._, Scalaz._
  """.stripMargin.trim
)

lazy val replJVM = repl.jvm
lazy val coreJS  = core.js
lazy val coreJVM = core.jvm
lazy val scalacheckJS  = scalacheck.js
lazy val scalacheckJVM = scalacheck.jvm
lazy val testsJS  = tests.js
lazy val testsJVM = tests.jvm
