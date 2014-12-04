import com.github.retronym.SbtOneJar.oneJar

organization := "com.slamdata.web"

name := "web"

mainClass in Compile := Some("slamdata.engine.api.Server")