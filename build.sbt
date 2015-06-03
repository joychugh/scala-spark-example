
name := "scala-spark-example"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1" % "provided",
  "org.apache.kafka" % "kafka_2.10" % "0.8.2.1",
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.3.1" % "provided"
    exclude("org.slf4j", "slf4j-api"),
  "junit" % "junit" % "4.12")

