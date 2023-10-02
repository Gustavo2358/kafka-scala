ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-scala"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.4.0",
  "com.typesafe.akka" %% "akka-actor" % "2.8.0"
)