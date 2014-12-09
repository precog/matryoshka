import sbt._
import Keys._


val scalazVersion     = "7.1.0"
val monocleVersion    = "0.5.0"
val unfilteredVersion = "0.8.1"

lazy val standardSettings = Defaults.defaultSettings ++ Seq(
  version := "1.1.1-SNAPSHOT",
  scalaVersion := "2.11.2",  
  logBuffered in Compile := false,
  logBuffered in Test := false,
  outputStrategy := Some(StdoutOutput),
  initialize := {
    assert(
      Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
        >= 7,
      "Java 7 or above required")
  },
  exportJars := true,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"), 
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  ),
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
    "org.scalaz"        %% "scalaz-core"               % scalazVersion              % "compile, test",
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion              % "compile, test",  
    "org.scalaz.stream" %% "scalaz-stream"             % "0.5a"                     % "compile, test",
    "org.spire-math"    %% "spire"                     % "0.8.2"                    % "compile, test",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion             % "compile, test",
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion             % "compile, test",
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion             % "compile, test",
    "org.threeten"      %  "threetenbp"                % "0.8.1"                    % "compile, test",
    "org.mongodb"       %  "mongo-java-driver"         % "2.12.2"                   % "compile, test",
    "net.databinder"    %% "unfiltered-filter"         % unfilteredVersion          % "compile, test",
    "net.databinder"    %% "unfiltered-netty-server"   % unfilteredVersion          % "compile, test",
    "net.databinder"    %% "unfiltered-netty"          % unfilteredVersion          % "compile, test",
    "io.argonaut"       %% "argonaut"                  % "6.1-M4"                   % "compile, test",
    "org.jboss.aesh"    %  "aesh"                      % "0.55"                     % "compile, test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion              % "compile, test",
    "com.github.julien-truffaut" %% "monocle-law"      % monocleVersion             % "compile, test",
    "org.scalacheck"    %% "scalacheck"                % "1.10.1"                   % "test",
    "org.specs2"        %% "specs2"                    % "2.3.13-scalaz-7.1.0-RC1"  % "test",
    "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"                   % "test"
  ),
  licenses += ("GNU Affero GPL V3", url("http://www.gnu.org/licenses/agpl-3.0.html")))

lazy val oneJarSettings = com.github.retronym.SbtOneJar.oneJarSettings ++ standardSettings

lazy val root = Project("root", file(".")) aggregate(core, web, it)

lazy val core = (project in file("core")) settings (oneJarSettings: _*)

lazy val web = (project in file("web")) dependsOn (core) settings (oneJarSettings: _*)

lazy val it = (project in file("it")) dependsOn (core, web) settings (standardSettings: _*)

lazy val admin = (project in file("admin")) dependsOn (core) settings (oneJarSettings: _*)
