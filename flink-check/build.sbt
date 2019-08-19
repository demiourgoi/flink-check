name := "flink-check"

organization := "es.ucm.fdi"

version := "0.0.3-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions  := Seq("2.11.8")

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

bintrayPackageLabels := Seq("testing", "scala", "apache flink")

bintrayVcsUrl := Some("git@github.com:demiourgoi/flink-check.git")

// Flink is ok with multiple execution contexts in the same JVM, but too much work in local
// mode leads to "Could not allocate enough memory segments for NetworkBufferPool (required (Mb)
// : 106, allocated (Mb): 97, missing (Mb): 9). Cause: Direct buffer memory (NetworkBufferPool.java:108)"
parallelExecution := false

lazy val sscheckVersion = "0.4.2-SNAPSHOT"

lazy val specs2Version = "3.8.4"

lazy val flinkVersion = "1.8.0"

lazy val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-runtime-web"  % flinkVersion % "provided",
  "org.apache.flink" %% "flink-connector-filesystem"  % flinkVersion % "provided",
  "org.apache.flink" %% "flink-hadoop-compatibility"  % flinkVersion % "provided"
)

libraryDependencies ++= Seq("org.scalacheck" %% "scalacheck" % "1.13.4", "org.scalacheck" %% "scalacheck" % "1.13.4" % "test")

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.26"

libraryDependencies += "org.specs2" %% "specs2-core" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-scalacheck" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-matcher-extra" % specs2Version

libraryDependencies += "org.specs2" %% "specs2-junit" % specs2Version

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.4" % "provided"

libraryDependencies ++= flinkDependencies

// leads to "noSuchMethodError: akka.actor.LocalActorRefProvider.log()Lakka/event/LoggingAdapter"
// due to multiple akka versions
libraryDependencies += "es.ucm.fdi" %% "sscheck-core" % sscheckVersion excludeAll(
  ExclusionRule(organization = "org.slf4j"),
  ExclusionRule(organization = "org.specs2"),
  ExclusionRule(organization = "org.scalatest"),
  ExclusionRule(organization = "org.scalacheck")
)

// show all the warnings: http://stackoverflow.com/questions/9415962/how-to-see-all-the-warnings-in-sbt-0-11
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

// sscheck repository
resolvers += Resolver.bintrayRepo("juanrh", "maven")
