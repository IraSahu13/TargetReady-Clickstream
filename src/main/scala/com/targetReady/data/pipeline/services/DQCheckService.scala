package com.targetReady.data.pipeline.services

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.targetReady.data.pipeline.variables.VariableDeclarations._
import com.targetReady.data.pipeline.services.DBService._
import com.targetReady.data.pipeline.dqCheck.DQCheckMethods._


object DQCheckService extends Logging {
  def executeDqCheck()(implicit spark: SparkSession): Unit = {

    /** READING DATA FROM MYSQL TABLE  */
    val dfReadStaged: DataFrame = sqlReader(JDBC_DRIVER, TABLE_NAME, JDBC_URL, USER_NAME, KEY_PASSWORD)(spark)


    /** CHECK NULL VALUES  */
    val dfCheckNull: Boolean = DqNullCheck(dfReadStaged, COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA)
    logInfo("Data quality null check completed.")


    /** CHECK DUPLICATE VALUES  */
    val dfCheckDuplicate: Boolean = DqDuplicateCheck(dfReadStaged, PRIMARY_KEY_CLICKSTREAM, EVENT_TIMESTAMP)
    logInfo("Data quality duplicate check completed.")


    /** WRITING TO PROD TABLE IN MYSQL  */
    if (dfCheckNull && dfCheckDuplicate) {
      sqlWriter(dfReadStaged, TABLE_NAME_FINAL, JDBC_URL)
      logInfo("Data write to production table complete.")
    }

  }

}
