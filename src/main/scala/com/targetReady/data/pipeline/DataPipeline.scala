package com.targetReady.data.pipeline

import com.targetReady.data.pipeline.variables.VariableDeclarations
import com.targetReady.data.pipeline.exceptions._
import com.targetReady.data.pipeline.services.PipelineService
import com.targetReady.data.pipeline.services.DQCheckService
import com.targetReady.data.pipeline.util.sparkSession.createSparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object DataPipeline extends Logging {
  var exitCode: Int = VariableDeclarations.FAILURE_EXIT_CODE

  def main(args: Array[String]): Unit = {

    /** Creating Spark Session  */
    val spark: SparkSession = createSparkSession()
    logInfo("Creating Spark Session complete.")


    /** Executing Pipeline  */
//    try {

      PipelineService.executePipeline()(spark)
      logInfo("Executing Pipeline complete.")
//      DQCheckService.executeDqCheck()(spark)
//      logInfo("Executing DqChecks complete.")

//    } catch {
//      case e: FileReaderException =>
//        logError("File read exception", e)
//
//      case e: FileWriterException =>
//        logError("file write exception", e)
//
//      case e: DqNullCheckException =>
//        logError("DQ check failed", e)
//
//      case e: DqDuplicateCheckException =>
//        logError("DQ check failed", e)
//
//      case e: Exception =>
//        logError("Unknown exception", e)
//    }
//    finally {
//      logInfo(s"Pipeline completed with status $exitCode")
////      spark.stop()
//      sys.exit(exitCode)
//    }

  }

}
