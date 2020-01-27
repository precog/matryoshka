import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}
import scoverage._
import sbt._
import Keys._

val MonocleVersion = "1.6.0"
val ScalazVersion = "7.2.30"
val Specs2Version = "4.8.2"

lazy val standardSettings = commonBuildSettings ++ Seq(
  logBuffered in Compile := false,
  logBuffered in Test := false,
  updateOptions := updateOptions.value.withCachedResolution(true),
  exportJars := true,
  organization := "com.slamdata",
  ScoverageKeys.coverageHighlighting := true,
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-implicits"),

  libraryDependencies ++= Seq(
    "com.slamdata"               %% "slamdata-predef" % "0.0.7",
    "com.github.julien-truffaut" %% "monocle-core"    % MonocleVersion,
    "org.scalaz"                 %% "scalaz-core"     % ScalazVersion,
    "org.typelevel"              %% "simulacrum"      % "1.0.0"),

  scalacOptions ++= {
    if (scalaVersion.value.startsWith("2.13"))
      Seq("-Ymacro-annotations")
    else
      Seq.empty
  },

  libraryDependencies ++= {
    if (!scalaVersion.value.startsWith("2.13"))
      Seq(compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full))
    else
      Seq.empty
  })

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
  .settings(console := (console in replJVM).value)
  .aggregate(
    coreJS,  scalacheckJS,  testsJS,
    coreJVM, scalacheckJVM, testsJVM,
    docs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = crossProject(JSPlatform, JVMPlatform).in(file("core"))
  .settings(name := "matryoshka-core")
  .settings(standardSettings ++ publishSettings: _*)
  .enablePlugins(AutomateHeaderPlugin)

lazy val scalacheck = crossProject(JSPlatform, JVMPlatform)
  .dependsOn(core)
  .settings(name := "matryoshka-scalacheck")
  .settings(standardSettings ++ publishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck"                % "1.14.3",
    "org.scalaz"     %% "scalaz-scalacheck-binding" % (ScalazVersion + "-scalacheck-1.14")))
  .enablePlugins(AutomateHeaderPlugin)

lazy val tests = crossProject(JSPlatform, JVMPlatform)
  .settings(name := "matryoshka-tests")
  .dependsOn(core, scalacheck)
  .settings(standardSettings ++ noPublishSettings: _*)
  .settings(libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %% "monocle-law"       % MonocleVersion % Test,
    "org.specs2"                 %% "specs2-core"       % Specs2Version  % Test,
    "org.specs2"                 %% "specs2-scalaz"     % Specs2Version  % Test,
    "org.typelevel"              %% "discipline-specs2" % "1.0.0"        % Test))
  .enablePlugins(AutomateHeaderPlugin)

lazy val docs = project
  .settings(name := "matryoshka-docs")
  .dependsOn(coreJVM)
  .settings(standardSettings ++ noPublishSettings: _*)
  .enablePlugins(MicrositesPlugin)
  .settings(
    micrositeName             := "Matryoshka",
    micrositeDescription      := "A library for doing bad-ass computer shit.",
    micrositeAuthor           := "SlamData",
    micrositeGithubOwner      := "slamdata",
    micrositeGithubRepo       := "matryoshka",
    micrositeBaseUrl          := "/matryoshka",
    micrositeDocumentationUrl := "/matryoshka/docs/01-Index.html",
    micrositeHighlightTheme   := "color-brewer")

/** A project just for the console.
  * Applies only the settings necessary for that purpose.
  */
lazy val repl = crossProject(JSPlatform, JVMPlatform) dependsOn (tests % "compile->test") settings standardSettings settings (
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
