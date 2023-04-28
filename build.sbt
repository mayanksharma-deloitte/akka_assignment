ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "akka_assignment"
  )
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.16"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.16"


