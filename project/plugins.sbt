resolvers += Resolver.url(
  "bintray-sbt-plugin-releases",
    url("http://dl.bintray.com/content/sbt/sbt-plugin-releases"))(
        Resolver.ivyStylePatterns)

// Temporary workaround for problem resolving scala 2.10.2 jars:
resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.1.0")

// WartRemover
addSbtPlugin("org.brianmckenna" % "sbt-wartremover" % "0.11")

// Scoverage:
resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.7.1")

// sbt-one-jar
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar" % "0.8")


// To generate Eclipse project files (run 'eclipse'):
//addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.4.0")

