name := "couchbasestreamswrapper"

organization := "io.dronekit"

version := "2.2.1"

scalaVersion := "2.11.7"

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
  val akkaStreamV = "2.0.1"
  val scalaTestV = "2.2.4"
  Seq(
    "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
    "com.typesafe.akka" %% "akka-stream-testkit-experimental" % akkaStreamV,
    "io.spray" %%  "spray-json" % "1.3.2",
    "ch.qos.logback" % "logback-classic" % "1.1.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
    "com.couchbase.client" % "java-client" % "2.2.3",
    "io.reactivex" % "rxjava-reactive-streams" % "1.0.1",
    "io.reactivex" %% "rxscala" % "0.25.0",
    "joda-time" % "joda-time" % "2.9.1",
    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}
