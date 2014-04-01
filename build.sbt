organization := "com.slamdata.slamengine"

name := "slamengine"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.3"

mainClass := Some("slamdata.engine.repl.Repl")

scalacOptions ++= Seq(
  "-feature",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"), Resolver.sonatypeRepo("snapshots"),
  "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/"
)

libraryDependencies ++= Seq(
  "org.scalaz"      %% "scalaz-core"                % "7.1.0-SNAPSHOT",
  "org.scalaz"      %% "scalaz-concurrent"          % "7.1.0-SNAPSHOT",  
  "org.scalaz"      %% "scalaz-task"                % "7.1.0-SNAPSHOT",  
  "org.threeten"    % "threetenbp"                  % "0.8.1",
  "org.mongodb"     % "mongo-java-driver"           % "2.11.4",
  "org.jboss.aesh"  % "aesh"                        % "0.48",
  "org.scalaz"      %% "scalaz-scalacheck-binding"  % "7.1.0-SNAPSHOT"  % "test",
  "org.scalacheck"  %% "scalacheck"                 % "1.10.1"  % "test",
  "org.specs2"      %% "specs2"                     % "2.3.4-scalaz-7.1.0-M3"   % "test"
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