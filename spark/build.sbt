name := "streaming-prediction"

version := "0.1"

scalaVersion := "2.12.15"
autoScalaLibrary := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0" //% "provided"
// libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0" % Test
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0"
// libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.0"
// libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.0.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.0.0"

