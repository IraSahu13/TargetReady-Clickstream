ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "TargetReady-Team2-ClickstreamDataPipeline"
  )
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.4.1"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.1"excludeAll(
  ExclusionRule(organization = "org.spark-project.spark", name = "unused"),
  ExclusionRule(organization = "org.apache.spark", name = "spark-streaming"),
  ExclusionRule(organization = "org.apache.hadoop")
)

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "12.4.0.jre11"

//logging library
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.29"

// https://mvnrepository.com/artifact/com.microsoft.azure/spark-mssql-connector
libraryDependencies += "com.microsoft.azure" %% "spark-mssql-connector" % "1.2.0"