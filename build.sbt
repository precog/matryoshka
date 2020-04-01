import sbtcrossproject.CrossPlugin.autoImport.{crossProject, CrossType}
import scoverage._
import sbt._
import Keys._

val MonocleVersion = "1.6.0"
val ScalazVersion = "7.2.30"
val Specs2Version = "4.8.2"

ThisBuild / organization := "com.slamdata"
ThisBuild / githubRepository := "matryoshka"

ThisBuild / homepage := Some(url("https://github.com/precog/matryoshka"))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/precog/matryoshka"),
    "scm:git@github.com:precog/matryoshka.git"))

ThisBuild / publishAsOSSProject := true

ThisBuild / crossScalaVersions := Seq("2.13.1", "2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

lazy val standardSettings = commonBuildSettings ++ Seq(
  logBuffered in Compile := false,
  logBuffered in Test := false,
  updateOptions := updateOptions.value.withCachedResolution(true),
  exportJars := true,
  ScoverageKeys.coverageHighlighting := true,
  scalacOptions in (Compile, doc) ++= Seq("-groups", "-implicits"),

  libraryDependencies ++= Seq(
    "com.slamdata"               %% "slamdata-predef" % "0.1.2",
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

lazy val root = Project("root", file("."))
  .settings(name := "matryoshka")
  .settings(standardSettings ++ noPublishSettings)
  .settings(console := (console in replJVM).value)
  .aggregate(
    coreJS,  scalacheckJS,  testsJS,
    coreJVM, scalacheckJVM, testsJVM,
    docs)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = crossProject(JSPlatform, JVMPlatform).in(file("core"))
  .settings(name := "matryoshka-core")
  .settings(standardSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val scalacheck = crossProject(JSPlatform, JVMPlatform)
  .dependsOn(core)
  .settings(name := "matryoshka-scalacheck")
  .settings(standardSettings)
  .settings(libraryDependencies ++= Seq(
    "org.scalacheck" %% "scalacheck"                % "1.14.3",
    "org.scalaz"     %% "scalaz-scalacheck-binding" % (ScalazVersion + "-scalacheck-1.14")))
  .enablePlugins(AutomateHeaderPlugin)

lazy val tests = crossProject(JSPlatform, JVMPlatform)
  .settings(name := "matryoshka-tests")
  .dependsOn(core, scalacheck)
  .settings(standardSettings ++ noPublishSettings)
  .settings(libraryDependencies ++= Seq(
    "com.github.julien-truffaut" %% "monocle-law"       % MonocleVersion % Test,
    "org.specs2"                 %% "specs2-core"       % Specs2Version  % Test,
    "org.specs2"                 %% "specs2-scalaz"     % Specs2Version  % Test,
    "org.typelevel"              %% "discipline-specs2" % "1.0.0"        % Test))
  .enablePlugins(AutomateHeaderPlugin)

lazy val docs = project
  .settings(name := "matryoshka-docs")
  .dependsOn(coreJVM)
  .settings(standardSettings ++ noPublishSettings)
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
