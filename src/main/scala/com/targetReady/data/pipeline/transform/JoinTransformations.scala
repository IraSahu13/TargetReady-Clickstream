package com.targetReady.data.pipeline.transform

import com.targetReady.data.pipeline.cleanser.Cleanser.dataTypeValidation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.targetReady.data.pipeline.variables.VariableDeclarations._

object JoinTransformations {
  /** ===============================================================================================================
   * FUNCTION TO JOIN TWO DATAFRAMES
   *
   *
   * @param df1      specifies the first dataframe taken as an input
   * @param df2      specifies the second dataframe taken as an input
   * @param joinKey  specifies the related joining column name between the two dataframes
   * @param joinType specifies the type of joining
   * @return         joined dataframe
   *         ============================================================================================================ */

  def joinTable(df1: DataFrame, df2: DataFrame, joinKey: String, joinType: String): DataFrame = {

    var df1WithWatermark = dataTypeValidation(df1, COLUMNS_VALID_DATATYPE_CLICKSTREAM, NEW_DATATYPE_CLICKSTREAM)
    df1WithWatermark = df1WithWatermark.withWatermark(EVENT_TIMESTAMP, "1 minute")

    val joinedDF = df1WithWatermark.join(df2, Seq(joinKey), joinType)

    joinedDF
  }
}
