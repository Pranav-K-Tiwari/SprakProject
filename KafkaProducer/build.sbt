
name := "KafkaProducer"

version := "0.1"

scalaVersion := "2.12.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % "provided"

// https://mvnrepository.com/artifact/com.twitter/hbc-twitter4j
libraryDependencies += "com.twitter" % "hbc-twitter4j" % "2.2.0"

// https://mvnrepository.com/artifact/com.twitter/hbc-core
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0"


