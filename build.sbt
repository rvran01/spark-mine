//
// http://spark.apache.org/docs/latest/quick-start.html#a-standalone-app-in-scala
//
name := """spark-mine"""

version := "1.0"

scalaVersion := "2.11.4"

resolvers ++= Seq(
"Maven Central Repo " at "http://repo1.maven.org/maven2/",
"Akka Repository" at "http://repo.akka.io/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"    % "1.2.0",
  "org.apache.hadoop"  % "hadoop-client" % "2.4.0"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
