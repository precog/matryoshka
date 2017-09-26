resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("io.get-coursier"   % "sbt-coursier"   % "1.0.0-RC12")
addSbtPlugin("com.fortysevendeg" % "sbt-microsites" % "0.3.3")
addSbtPlugin("org.scala-js"      % "sbt-scalajs"    % "0.6.20")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"  % "1.5.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-site"       % "1.3.1")
addSbtPlugin("com.slamdata"      % "sbt-slamdata"   % "0.5.1")
addSbtPlugin("com.eed3si9n"      % "sbt-unidoc"     % "0.4.1")
