import com.github.retronym.SbtOneJar.oneJar

organization := "com.slamdata.slamengine"

name := "slamengine"

mainClass in Compile := Some("slamdata.engine.repl.Repl")

fork in run := true

connectInput in run := true

outputStrategy := Some(StdoutOutput)

// TODO: These are preexisting problems that need to be fixed. DO NOT ADD MORE.
wartremoverExcluded ++= Seq(
  "slamdata.engine.analysis.Analysis",
  "slamdata.engine.analysis.AnnotatedTree",
  "slamdata.engine.analysis.term.Term",
  "slamdata.engine.analysis.Tree",
  "slamdata.engine.PartialFunctionOps",
  "slamdata.engine.physical.mongodb.Bson.Null", // uses null, and has to
  "slamdata.engine.physical.mongodb.BsonField",
  "slamdata.engine.physical.mongodb.MongoDbExecutor",
  "slamdata.engine.physical.mongodb.MongoWrapper")

// Disable wartremover for faster builds, unless running under Travis/Jenkins:  
wartremoverExcluded ++= scala.util.Properties.envOrNone("ENABLE_WARTREMOVER").fold("slamdata.engine" :: Nil)(_ => Nil)

// TODO: These are preexisting problems that need to be fixed. DO NOT ADD MORE.
wartremoverErrors in (Compile, compile) ++= Warts.allBut(
  Wart.Any,
  Wart.AsInstanceOf,
  Wart.DefaultArguments,
  Wart.IsInstanceOf,
  Wart.NoNeedForMonad,
  Wart.NonUnitStatements,
  Wart.Nothing,
  Wart.Product,
  Wart.Serializable)

import ScoverageSbtPlugin._

ScoverageKeys.coverageExcludedPackages := "slamdata.engine.repl;.*RenderTree;.*MongoDbExecutor;.*MongoWrapper"

ScoverageKeys.coverageMinimum := 66

ScoverageKeys.coverageFailOnMinimum := true

ScoverageKeys.coverageHighlighting := true

