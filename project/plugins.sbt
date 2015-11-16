resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"
libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

addSbtPlugin("org.scoverage"         % "sbt-scoverage"   % "1.3.3")
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar"      % "0.8")
addSbtPlugin("com.github.gseitz"     % "sbt-release"     % "0.8.5")
addSbtPlugin("org.brianmckenna"      % "sbt-wartremover" % "0.14")
addSbtPlugin("de.heikoseeberger"     % "sbt-header"      % "1.5.0")
addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo"   % "0.5.0")
