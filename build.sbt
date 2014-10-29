organization := "com.slamdata.slamengine"

name := "slamengine"

version := "1.0.2-SNAPSHOT"

scalaVersion := "2.11.2"

initialize := {
  assert(
    Integer.parseInt(sys.props("java.specification.version").split("\\.")(1))
      >= 7,
    "Java 7 or above required")
}

// mainClass in (run) := Some("slamdata.engine.repl.Repl")

// mainClass in (packageBin) := Some("slamdata.engine.api.Server")

// mainClass in (oneJar) := Some("slamdata.engine.api.Server")


fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

Defaults.itSettings

lazy val itConfigProject = project in file(".") configs(IntegrationTest)

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

instrumentSettings

ScoverageKeys.minimumCoverage := 61

ScoverageKeys.failOnMinimumCoverage := true

CoverallsPlugin.coverallsSettings

com.github.retronym.SbtOneJar.oneJarSettings

val scalazVersion = "7.1.0"
val monocleVersion = "0.5.0"
val unfilteredVersion = "0.8.1"

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"               % scalazVersion,
  "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion,  
  "org.scalaz.stream" %% "scalaz-stream"             % "0.5a",
  "org.spire-math"    %% "spire"                     % "0.8.2",
  "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion,
  "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion,
  "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion,
  "org.threeten"      %  "threetenbp"                % "0.8.1",
  "org.mongodb"       %  "mongo-java-driver"         % "2.12.2",
  "net.databinder"    %% "unfiltered-filter"         % unfilteredVersion,
  "net.databinder"    %% "unfiltered-netty-server"   % unfilteredVersion,
  "net.databinder"    %% "unfiltered-netty"          % unfilteredVersion,
  "io.argonaut"       %% "argonaut"                  % "6.1-M4",
  "org.jboss.aesh"    %  "aesh"                      % "0.55",
  "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion             % "test",
  "com.github.julien-truffaut" %% "monocle-law"      % monocleVersion            % "test",
  "org.scalacheck"    %% "scalacheck"                % "1.10.1"                  % "test",
  "org.specs2"        %% "specs2"                    % "2.3.13-scalaz-7.1.0-RC1" % "it,test",
  "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"                  % "test"
)

// To run tests in Eclipse:
libraryDependencies ++= Seq(  
  "org.specs2"        %% "specs2-junit"              % "2.3.13-scalaz-7.1.0-RC1" % "test"
)

seq(bintraySettings:_*)

publishMavenStyle := true

licenses += ("GNU Affero GPL V3", url("http://www.gnu.org/licenses/agpl-3.0.html"))

bintray.Keys.packageLabels in bintray.Keys.bintray :=
  Seq("mongodb", "nosql analytics", "sql", "analytics", "scala")

publishTo <<= (version).apply { v =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT"))
    Some("Snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("Releases" at nexus + "service/local/staging/deploy/maven2")
}

credentials += {
  Seq("build.publish.user", "build.publish.password").map(k => Option(System.getProperty(k))) match {
    case Seq(Some(user), Some(pass)) =>
      Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", user, pass)
    case _ =>
      Credentials(Path.userHome / ".ivy2" / ".credentials")
  }
}

pomIncludeRepository := Function.const(false)

pomExtra := (
  <url>http://github.com/slamdata/slamengine</url>
  <licenses>
      <license>
          <name>GNU Affero General Public License Version 3</name>
          <distribution>repo</distribution>
          <url>http://www.gnu.org/licenses/agpl-3.0.html</url>
      </license>
  </licenses>
  <scm>
    <url>https://github.com/slamdata/slamengine</url>
    <connection>scm:git:git://github.com/slamdata/slamengine.git</connection>
    <developerConnection>scm:git:git@github.com:slamdata/slamengine.git</developerConnection>
  </scm>
  <developers>
    <developer>
      <id>jdegoes</id>
      <name>John A. De Goes</name>
      <url>https://degoes.net</url>
    </developer>
  </developers>
)
