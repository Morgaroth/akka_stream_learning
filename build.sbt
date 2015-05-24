name := "akka-stream-scala"

version := "1.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-RC3" withSources()
)

fork in run := true
