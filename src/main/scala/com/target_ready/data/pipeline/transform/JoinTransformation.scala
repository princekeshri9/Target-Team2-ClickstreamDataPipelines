package com.target_ready.data.pipeline.transform

import com.target_ready.data.pipeline.clenser.Clenser.dataTypeValidation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.target_ready.data.pipeline.constants.ApplicationConstants._

object JoinTransformation {

  def joinTable(df1: DataFrame, df2: DataFrame, joinKey: String, joinType: String): DataFrame = {

//    var df1WithWatermark=dataTypeValidation(df1, COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)
//    df1WithWatermark= df1WithWatermark.withWatermark(EVENT_TIMESTAMP, "1 minute")

    val joinedDF = df1.join(df2, Seq(joinKey), joinType)
    joinedDF
  }

}
