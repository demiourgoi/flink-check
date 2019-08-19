ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal,
    Resolver.bintrayRepo("juanrh", "maven")
)

name := "flink-check-examples"

version := "0.0.3-SNAPSHOT"

organization := "es.ucm.fdi"

ThisBuild / scalaVersion := "2.11.8"

// Avoid OutOfMemoryError caused by "Could not allocate enough memory segments for NetworkBufferPool"
// by running properties one by one. Note this is running using local execution environments
parallelExecution := false

lazy val flinkVersion = "1.8.0"

lazy val flinkCheckVersion = "0.0.3-SNAPSHOT"

lazy val slf4jVersion = "1.7.15"

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-runtime-web"  % flinkVersion % "provided"
)

lazy val specs2Version = "3.8.4"

lazy val specs2Dependencies = Seq(
  "org.specs2" %% "specs2-core" % specs2Version % "test",
  "org.specs2" %% "specs2-scalacheck" % specs2Version % "test",
  "org.specs2" %% "specs2-matcher-extra" % specs2Version % "test",
  "org.specs2" %% "specs2-junit" % specs2Version % "test"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    libraryDependencies ++= specs2Dependencies,
    libraryDependencies += "es.ucm.fdi" %% "flink-check" % flinkCheckVersion,
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion % "provided",
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion % "provided"
    )
  )

assembly / mainClass := Some("es.ucm.fdi.sscheck.flink.demo.Pollution")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
