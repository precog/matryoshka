resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"
libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

addSbtPlugin("de.heikoseeberger"     % "sbt-header"      % "1.5.0")
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar"      % "0.8")
addSbtPlugin("com.jsuereth"          % "sbt-pgp"         % "1.0.0")
addSbtPlugin("com.github.gseitz"     % "sbt-release"     % "1.0.0")
addSbtPlugin("org.scala-js"          % "sbt-scalajs"     % "0.6.9")
addSbtPlugin("org.scoverage"         % "sbt-scoverage"   % "1.3.3")
addSbtPlugin("com.typesafe.sbt"      % "sbt-site"        % "1.0.0")
addSbtPlugin("com.eed3si9n"          % "sbt-unidoc"      % "0.3.3")
addSbtPlugin("org.brianmckenna"      % "sbt-wartremover" % "0.14")
