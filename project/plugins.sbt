resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"
libraryDependencies += "org.kohsuke" % "github-api" % "1.77"

addSbtPlugin("de.heikoseeberger"     % "sbt-header"      % "1.6.0")
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar"      % "0.8")
addSbtPlugin("com.jsuereth"          % "sbt-pgp"         % "1.0.0")
addSbtPlugin("com.github.gseitz"     % "sbt-release"     % "1.0.3")
addSbtPlugin("org.scala-js"          % "sbt-scalajs"     % "0.6.13")
addSbtPlugin("org.scoverage"         % "sbt-scoverage"   % "1.4.0")
addSbtPlugin("com.typesafe.sbt"      % "sbt-site"        % "1.1.0")
addSbtPlugin("com.eed3si9n"          % "sbt-unidoc"      % "0.3.3")
addSbtPlugin("org.brianmckenna"      % "sbt-wartremover" % "0.14")
addSbtPlugin("io.get-coursier"       % "sbt-coursier"    % "1.0.0-M14")
