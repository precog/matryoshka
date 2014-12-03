organization := "com.slamdata.slamengine"

name := "it"

version := "1.1.1-SNAPSHOT"

scalaVersion := "2.11.2"

initialize := {
  assert(
    Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
      >= 7,
    "Java 7 or above required")
}

val scalazVersion     = "7.1.0"
val monocleVersion    = "0.5.0"
val unfilteredVersion = "0.8.1"

fork in Test := true

parallelExecution in Test := false

logBuffered in Test := false

javaOptions in Test += "-Xmx8G"

outputStrategy := Some(StdoutOutput)

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"               % scalazVersion              % "test",
  "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion              % "test",  
  "org.scalaz.stream" %% "scalaz-stream"             % "0.5a"                     % "test",
  "org.spire-math"    %% "spire"                     % "0.8.2"                    % "test",
  "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion             % "test",
  "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion             % "test",
  "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion             % "test",
  "org.threeten"      %  "threetenbp"                % "0.8.1"                    % "test",
  "org.mongodb"       %  "mongo-java-driver"         % "2.12.2"                   % "test",
  "net.databinder"    %% "unfiltered-filter"         % unfilteredVersion          % "test",
  "net.databinder"    %% "unfiltered-netty-server"   % unfilteredVersion          % "test",
  "net.databinder"    %% "unfiltered-netty"          % unfilteredVersion          % "test",
  "io.argonaut"       %% "argonaut"                  % "6.1-M4"                   % "test",
  "org.jboss.aesh"    %  "aesh"                      % "0.55"                     % "test",
  "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion              % "test",
  "com.github.julien-truffaut" %% "monocle-law"      % monocleVersion             % "test",
  "org.scalacheck"    %% "scalacheck"                % "1.10.1"                   % "test",
  "org.specs2"        %% "specs2"                    % "2.3.13-scalaz-7.1.0-RC1"  % "test",
  "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"                   % "test"
)

// TODO: These are preexisting problems that need to be fixed. DO NOT ADD MORE.
wartremoverExcluded ++= Seq(
  "slamdata.engine.analysis.Analysis",
  "slamdata.engine.analysis.AnnotatedTree",
  "slamdata.engine.analysis.term.Term",
  "slamdata.engine.analysis.Tree",
  "slamdata.engine.PartialFunctionOps",
  "slamdata.engine.physical.mongodb.Bson.Null", // uses null, and has to
  "slamdata.engine.physical.mongodb.BsonField",
  "slamdata.engine.physical.mongodb.MongoDbExecutor",
  "slamdata.engine.physical.mongodb.MongoWrapper")

// Disable wartremover for faster builds, unless running under Travis/Jenkins:  
wartremoverExcluded ++= scala.util.Properties.envOrNone("ENABLE_WARTREMOVER").fold("slamdata.engine" :: Nil)(_ => Nil)

// TODO: These are preexisting problems that need to be fixed. DO NOT ADD MORE.
wartremoverErrors in (Compile, compile) ++= Warts.allBut(
  Wart.Any,
  Wart.AsInstanceOf,
  Wart.DefaultArguments,
  Wart.IsInstanceOf,
  Wart.NoNeedForMonad,
  Wart.NonUnitStatements,
  Wart.Nothing,
  Wart.Product,
  Wart.Serializable)

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"), 
  Resolver.sonatypeRepo("snapshots"),
  "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
)

import ScoverageSbtPlugin._

ScoverageKeys.coverageExcludedPackages := "slamdata.engine.repl;.*RenderTree;.*MongoDbExecutor;.*MongoWrapper"

ScoverageKeys.coverageMinimum := 66

ScoverageKeys.coverageFailOnMinimum := true

ScoverageKeys.coverageHighlighting := true

// CoverallsPlugin.coverallsSettings

licenses += ("GNU Affero GPL V3", url("http://www.gnu.org/licenses/agpl-3.0.html"))
