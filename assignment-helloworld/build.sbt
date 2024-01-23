name := "HelloWorld"

version := "1.0"

scalaVersion := "2.13.4"
sbtVersion :="1.7.1"
Compile / doc / scalacOptions ++= Seq("-Vimplicits", "-deprecation", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused")

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"


val sprayGroup = "io.spray"
val sprayJsonVersion = "1.3.5"
libraryDependencies ++= List("spray-json") map { c => sprayGroup %% c % sprayJsonVersion }