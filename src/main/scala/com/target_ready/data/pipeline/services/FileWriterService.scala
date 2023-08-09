package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.FileWriterException
import com.target_ready.data.pipeline.constants.ApplicationConstants.{CHECKPOINT_LOCATION, SERVER_ID}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode,Trigger}

object FileWriterService {

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
      case e: Exception => FileWriterException("Unable to write files to the location: " + topic)
    }
  }


  def writeDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
        .awaitTermination(10000)

    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }

  def writeDataToSqlServer(df: DataFrame, driver: String, tableName: String, jdbcUrl: String, user: String, password: String): Unit = {
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
      .start().awaitTermination(150000)
  }

  def writeNullDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(Trigger.Once())
        .start()
        .awaitTermination(10000)
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }
}
