
name := "WindowAggregate"

version := "1.2"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-hive-thriftserver" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.4.1",
  "org.json4s" %% "json4s-native" % "3.2.10"
)

// META-INF discarding
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}