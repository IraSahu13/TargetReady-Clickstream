package com.targetReady.data.pipeline.services

import org.apache.spark.sql.{DataFrame,SparkSession}
import com.targetReady.data.pipeline.exceptions.FileReaderException
import com.targetReady.data.pipeline.variables.VariableDeclarations.SERVER_ID

object FileReaderService {

  /** ==============================================================================================================
   *  FUNCTION TO READ DATA FROM SOURCE DIR
   *
   *  @param filePath          specifies the location where null values will be written
   *  @param fileFormat        specifies the format of file
   *  @return                  dataframe of input data
   *  ============================================================================================================ */

  def readFromFile(filePath: String, fileFormat: String)(implicit spark: SparkSession): DataFrame = {

    val readFileData_df: DataFrame =
      try {
        spark
          .read
          .format(fileFormat)
          .option("header", "true")
          .load(filePath)
      }
      catch {
        case e: Exception => {
          FileReaderException("Unable to read input file from the given location: " + filePath)
          spark.emptyDataFrame
        }
      }

    //  Checking for empty input file
    val readFileDataCount: Long = readFileData_df.count()
    if (readFileDataCount == 0)  throw FileReaderException("Input File is empty: " + filePath)

    readFileData_df
  }




  /** ==============================================================================================================
   *  FUNCTION TO LOAD DATA FROM KAFKA STREAM
   *
   *  @param topic    specifies the kafka topic name
   *  @return         dataframe of loaded data
   *  ============================================================================================================ */

  def loadDataFromStream(topic: String)(implicit spark: SparkSession): DataFrame = {
    val readKafkaStreamData_df: DataFrame = {
      try {
        spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", SERVER_ID)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load()
      }
      catch {
        case e: Exception => {
          FileReaderException("Unable to load data from kafka topic: " + topic)
          spark.emptyDataFrame
        }
      }
    }

    readKafkaStreamData_df
  }
}