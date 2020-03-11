resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtCoursier

addSbtPlugin("org.portable-scala" % "sbt-scalajs-crossproject" % "0.6.0")

addSbtPlugin("com.47deg"         % "sbt-microsites" % "0.8.0")
addSbtPlugin("org.scalameta"     % "sbt-mdoc"       % "1.2.10")
addSbtPlugin("org.scala-js"      % "sbt-scalajs"    % "0.6.26")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"  % "1.5.1")
addSbtPlugin("com.typesafe.sbt"  % "sbt-site"       % "1.3.2")
addSbtPlugin("com.eed3si9n"      % "sbt-unidoc"     % "0.4.2")
addSbtPlugin("com.slamdata" % "sbt-slamdata" % "6.2.7")
