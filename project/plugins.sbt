// Temporary workaround for problem resolving scala 2.10.2 jars:
resolvers += Resolver.sonatypeRepo("releases")

// Standard sbt plugins:
resolvers += Resolver.url(
  "sbt-plugin-releases",
  new URL("http://dl.bintray.com/content/sbt/sbt-plugin-releases")
)(Resolver.ivyStylePatterns)

// Jenkins CI
resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"

libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

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

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.4.0")
