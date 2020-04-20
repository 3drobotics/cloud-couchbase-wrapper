name := "couchbasestreamswrapper"

organization := "io.dronekit"

version := "3.1.0"

scalaVersion := "2.13.1"
crossScalaVersions := Seq("2.12.10", "2.13.1")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature", "-language:postfixOps")

isSnapshot := true

libraryDependencies ++= {
  val akkaV = "2.6.4"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test,
    "com.typesafe.play" %% "play-json" % "2.8.1",
    "com.couchbase.client" % "java-client" % "2.7.14",
    "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}
