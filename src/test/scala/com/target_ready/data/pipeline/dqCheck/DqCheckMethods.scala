package com.target_ready.data.pipeline.dqCheck

import org.apache.spark.sql.{Column,DataFrame}
import org.apache.spark.sql.functions._
import com.target_ready.data.pipeline.exceptions.{DqNullCheckException,DqDupCheckException}
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import org.apache.spark.sql.expressions.Window

object DqCheckMethods {

  def dqNullCheck(df: DataFrame, keyColumns: Seq[String]): Boolean = {

    val columnNames: Seq[Column] = keyColumns.map(c => col(c))
    val condition: Column = columnNames.map(c => c.isNull || c === "" || c.contains("NULL") || c.contains("null")).reduce(_ || _)
    val dfCheckNullKeyRows: DataFrame = df.withColumn("nullFlag", when(condition, value = true).otherwise(value = false))

    val nullDf: DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag") === true)
    if (nullDf.count() > 0) throw DqNullCheckException("The file contains nulls")
    true

  }

  def DqDuplicateCheck(df: DataFrame, KeyColumns: Seq[String], orderByCol: String): Boolean = {

    val windowSpec = Window.partitionBy(KeyColumns.map(col): _*).orderBy(desc(orderByCol))
    val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
      .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)

    if (df.count() != dfDropDuplicate.count()) throw DqDupCheckException("The file contains duplicate")
    true
  }
}
