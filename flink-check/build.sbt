name := "flink-check"

organization := "es.ucm.fdi"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.6"

crossScalaVersions  := Seq("2.10.6")

lazy val sscheckVersion = "0.3.2" 

// show all the warnings: http://stackoverflow.com/questions/9415962/how-to-see-all-the-warnings-in-sbt-0-11
scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation")

// sscheck repository
resolvers += Resolver.bintrayRepo("juanrh", "maven")
