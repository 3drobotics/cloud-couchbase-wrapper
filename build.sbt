name := "couchbasestreamswrapper"

organization := "io.dronekit"

version := "3.0.0"

scalaVersion := "2.13.1"
crossScalaVersions := Seq("2.12.10", "2.13.1")

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature", "-language:postfixOps")

resolvers += "Artifactory" at "https://dronekit.artifactoryonline.com/dronekit/libs-snapshot-local/"

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

isSnapshot := true

publishTo := {
  val artifactory = "https://dronekit.artifactoryonline.com/"
  if (isSnapshot.value)
    Some("snapshots" at artifactory + s"dronekit/libs-snapshot-local;build.timestamp=${new java.util.Date().getTime}")
  else
    Some("snapshots" at artifactory + "dronekit/libs-release-local")
}

libraryDependencies ++= {
  val akkaV = "2.5.25"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test,
    "com.typesafe.play" %% "play-json" % "2.7.4",
    "com.couchbase.client" % "java-client" % "2.7.14",
    "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test
  )
}
