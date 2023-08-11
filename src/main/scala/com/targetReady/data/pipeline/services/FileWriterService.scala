package com.targetReady.data.pipeline.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger._
import com.targetReady.data.pipeline.exceptions.FileWriterException
import com.targetReady.data.pipeline.variables.VariableDeclarations.{CHECKPOINT_PATH, SERVER_ID}
import org.apache.spark.sql.streaming.OutputMode

object FileWriterService {

  /** ==================================================================================================================
   * FUNCTION TO WRITE DATA INTO KAFKA STREAM
   *
   * @param df    specifies the dataframe to be written
   * @param topic specifies the kafka topic name where the dataframe will be written
   * ===================================================================================================================  */

  def writeDataToStream(df: DataFrame, topic: String): Unit = {
    try {
      df
        .selectExpr("CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", SERVER_ID)
        .option("topic", topic)
        .save()
    } catch {
      case e: Exception => FileWriterException("Unable to write dataframe to kafka topic: " + topic)
    }
  }


  /**  =================================================================================================================
   * FUNCTION TO WRITE DATA INTO OUTPUT LOCATION IN REQUIRED FORMAT
   *
   * @param df         specifies the dataframe to be written
   * @param filePath   specifies the location of the output file
   * @param fileFormat specifies the format of the file
   * ================================================================================================================== */

  def writeDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
        .awaitTermination(10000)

    } catch {
      case e: Exception => FileWriterException("Unable to write dataframe to the output location: " + filePath)
    }
  }


  /**  =================================================================================================================
   * FUNCTION TO SAVE DATA INTO SQL TABLE
   *
   * @param df        specifies the dataframe to be written
   * @param driver    MySql driver
   * @param tableName specifies the MySql table name
   * @param jdbcUrl   specifies the jdbc URL
   * @param user      specifies the MySql database username
   * @param password  specifies the MySql database password
   * ================================================================================================================== */

  def writeDataToSqlServer(df: DataFrame, driver: String, tableName: String, jdbcUrl: String, user: String, password: String): Unit = {
    try {
      df.writeStream
        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
          batchDF.write
            .format("jdbc")
            .option("driver", driver)
            .option("url", jdbcUrl)
            .option("dbtable", tableName)
            .option("user", user)
            .option("password", password)
            .mode("overwrite")
            .save()
        }
        .outputMode(OutputMode.Append())
        .start()
        .awaitTermination(60000)
    } catch {
      case e: Exception => FileWriterException("Unable to write dataframe to MySQL Server: ")
    }
  }


  /**  =================================================================================================================
   * FUNCTION TO SAVE NULL-VALUE DATA INTO NULL-VALUE-OUTPUT LOCATION
   *
   * @param df         specifies the dataframe to be written
   * @param filePath   specifies the location where null values will be written
   * @param fileFormat specifies teh format of the file
   * ================================================================================================================== */

  def writeNullDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
        .awaitTermination(10000)
    } catch {
      case e: Exception => FileWriterException("Unable to write null values to the location: " + filePath)
    }
  }
}