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
  "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
  "junit" % "junit" % "4.8.1" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark"  %% "spark-core"    % "1.2.0",
  "org.apache.hadoop"  % "hadoop-client" % "2.4.0"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")
