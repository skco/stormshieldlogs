ThisBuild / version := "0.1.0-SNAPSHOT"



ThisBuild / scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
)