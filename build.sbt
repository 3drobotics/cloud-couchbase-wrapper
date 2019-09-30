name := "couchbasestreamswrapper"

organization := "io.dronekit"

version := "2.8.0"

scalaVersion := "2.12.8"

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
  val akkaV = "2.5.23"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test,
    "com.typesafe.play" %% "play-json" % "2.7.4",
    "com.couchbase.client" % "java-client" % "2.5.8",
    "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
    "org.scalatest" %% "scalatest" % "3.0.1" % Test
  )
}
