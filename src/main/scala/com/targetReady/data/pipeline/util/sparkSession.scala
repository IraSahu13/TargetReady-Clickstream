package com.targetReady.data.pipeline.util

import org.apache.spark.sql.SparkSession
import  com.targetReady.data.pipeline.variables.VariableDeclarations.{APP_NAME,MASTER_SERVER}

object sparkSession {

  /** Function to Create Spark Session */

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName(APP_NAME)
      .master(MASTER_SERVER)
      .getOrCreate()
  }
}