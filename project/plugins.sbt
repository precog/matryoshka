// Temporary workaround for problem resolving scala 2.10.2 jars:
resolvers += Resolver.sonatypeRepo("releases")

// Standard sbt plugins:
resolvers += Classpaths.sbtPluginReleases

// Jenkins CI
resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"

libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

resolvers += Resolver.url(
  "sbt-plugin-releases",
  new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/")
)(Resolver.ivyStylePatterns)

// Scoverage & Coveralls:
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0.BETA1")

// sbt-one-jar
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar" % "0.8")

// sbt-release
addSbtPlugin("com.github.gseitz" % "sbt-release" % "0.8.5")

// wartremover
addSbtPlugin("org.brianmckenna" % "sbt-wartremover" % "0.13")

// sbt-header
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.5.0")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.5.0")
