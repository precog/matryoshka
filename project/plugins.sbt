// Temporary workaround for problem resolving scala 2.10.2 jars:
resolvers += Resolver.sonatypeRepo("releases")

// Standard sbt plugins:
resolvers += Classpaths.sbtPluginReleases

// Scoverage & Coveralls:
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.0.1")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0.BETA1")

// sbt-one-jar
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar" % "0.8")