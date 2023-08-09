package com.target_ready.data.pipeline.services
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import org.apache.spark.sql._


object DbService {
  def sqlWriter(df: DataFrame, tableName: String, url: String): Unit = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", USER_NAME)
    connectionProperties.put("password", KEY_PASSWORD)
    connectionProperties.put("driver", JDBC_DRIVER)

    df.write.mode(SaveMode.Overwrite).jdbc(url, tableName, connectionProperties)
  }

  def sqlReader(driver: String, tableName: String, jdbcUrl: String, user: String, password: String)(implicit spark: SparkSession): DataFrame = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", driver)

    val df: DataFrame = spark.read.jdbc(jdbcUrl, tableName, connectionProperties)
    df
  }
}
