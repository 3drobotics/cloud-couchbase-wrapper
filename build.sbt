name := "couchbasestreamswrapper"

organization := "io.dronekit"

version := "2.7.2"

scalaVersion := "2.12.6"

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
  val akkaV = "2.5.12"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV,
    "io.spray" %%  "spray-json" % "1.3.3",
    "com.typesafe.play" %% "play-json" % "2.6.7",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    "com.couchbase.client" % "java-client" % "2.5.8",
    "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
    "io.reactivex" %% "rxscala" % "0.26.5",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "com.amazonaws" % "aws-java-sdk-ec2" % "1.11.269"
  )
}
