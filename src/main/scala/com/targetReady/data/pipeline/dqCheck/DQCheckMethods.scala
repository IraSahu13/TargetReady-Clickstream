package com.targetReady.data.pipeline.dqCheck

import com.targetReady.data.pipeline.variables.VariableDeclarations.ROW_NUMBER
import com.targetReady.data.pipeline.exceptions.{DqDuplicateCheckException, DqNullCheckException}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, when}

object DQCheckMethods {
  /** ===============================================================================================================
   * FUNCTION TO CHECK FOR NULL VALUES
   *
   *
   * @param df        the dataframe taken as an input
   * @param keyColumns column names on which the checks need to be performed
   * @return true: if no null value is found and vice-versa
   *         ============================================================================================================= */

  def DqNullCheck(df: DataFrame, keyColumns: Seq[String]): Boolean = {
    val columnNames: Seq[Column] = keyColumns.map(x => col(x))
    val condition: Column = columnNames.map(x => x.isNull).reduce(_ || _)
    val dfCheckNullKeyRows: DataFrame = df.withColumn("nullFlag", when(condition, value = true).otherwise(value = false))

    val nullDf: DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag") === true)

    if (nullDf.count() > 0)
      throw DqNullCheckException("The file contains nulls")
    true
  }

  /** ==============================================================================================================
   * FUNCTION TO CHECK FOR DUPLICATE VALUES
   *
   *
   * @param df         the dataframe
   * @param KeyColumns sequence of key columns of the df dataframe
   * @param orderByColumn
   * @return true: if no duplicate value is found and vice-versa
   *         ============================================================================================================= */

  def DqDuplicateCheck(df: DataFrame, KeyColumns: Seq[String], orderByCol: String): Boolean = {
    val windowSpec = Window.partitionBy(KeyColumns.map(col): _*).orderBy(desc(orderByCol))
    val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
      .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
    if (df.count() != dfDropDuplicate.count())
      throw DqDuplicateCheckException("The file contains duplicates")
    true

  }
}
