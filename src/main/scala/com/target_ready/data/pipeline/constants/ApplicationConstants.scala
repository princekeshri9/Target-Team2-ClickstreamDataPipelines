package com.target_ready.data.pipeline.constants

object ApplicationConstants {

  val SERVER_ID: String = "localhost:9092"
  val APP_NAME: String = "TargetReady-Team2-ClickStreamDataPipeline"
  val MASTER_SERVER: String = "local[*]"

  val TOPIC_NAME_ITEM_DATA: String = "itemDataTest"
  val TOPIC_NAME_CLICKSTREAM_DATA: String = "clickStreamDataTest"

  val CHECKPOINT_LOCATION: String = "checkpoint"

  val INPUT_FORMAT_ITEM_DATA: String = "csv"
  val OUTPUT_FORMAT_ITEM_DATA: String = "orc"
  val INPUT_FILE_PATH_ITEM_DATA: String = "data/Input/item/item_data.csv"

  val INPUT_FORMAT_CLICKSTREAM: String = "csv"
  val OUTPUT_FORMAT__CLICKSTREAM: String = "orc"
  val INPUT_FILE_PATH_CLICKSTREAM_DATA: String = "data/Input/clickstream/clickstream_log.csv"

  val OUTPUT_FILE_PATH: String = "data/output"
  val NULL_VALUE_PATH: String = "data/output/null_value_output/null_outputs.orc"

  val NULL_VALUE_FILE_FORMAT: String = "orc"

  val DUP_VALUE_CHECK_COLUMN: String = "item_id"
  val ITEM_ID: String = "item_id"
  val ITEM_PRICE: String = "item_price"
  val PRODUCT_TYPE: String = "product_type"
  val DEPARTMENT_NAME: String = "department_name"
  val COLUMNS_PRIMARY_KEY_ITEM_DATA: Seq[String] = Seq(ApplicationConstants.ITEM_ID)
  val COLUMN_NAMES_ITEM_DATA: List[String] = List(ITEM_ID, ITEM_PRICE, PRODUCT_TYPE, DEPARTMENT_NAME)
  val COLUMNS_CHECK_NULL_DQ_CHECK_ITEM_DATA: Seq[String] = Seq(ApplicationConstants.ITEM_ID)

  val ID: String = "id"
  val EVENT_TIMESTAMP: String = "event_timestamp"
  val DEVICE_TYPE: String = "device_type"
  val SESSION_ID: String = "session_id"
  val VISITOR_ID: String = "visitor_id"
  val REDIRECTION_SOURCE: String = "redirection_source"
  val VALUE: String = "value"
  val COLUMNS_PRIMARY_KEY_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID)
  val COLUMN_NAMES_CLICKSTREAM_DATA: Seq[String] = Seq(ApplicationConstants.ID, ApplicationConstants.EVENT_TIMESTAMP, ApplicationConstants.DEVICE_TYPE, ApplicationConstants.SESSION_ID, ApplicationConstants.VISITOR_ID, ApplicationConstants.ITEM_ID, ApplicationConstants.REDIRECTION_SOURCE)
  val COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ID, ApplicationConstants.VISITOR_ID)
  val EVENT_TIMESTAMP_OPTION: String = "event_timestamp"

  val TIMESTAMP_DATATYPE: String = "timestamp"
  val TTIMESTAMP_FORMAT: String = "MM/dd/yyyy H:mm"

  val COLUMNS_VALID_DATATYPE_CLICKSTREAM: Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)
  val COLUMNS_VALID_DATATYPE_ITEM: Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)

  val NEW_DATATYPE_CLICKSTREAM: Seq[String] = Seq("timestamp")
  val NEW_DATATYPE_ITEM: Seq[String] = Seq("float")

  val JOIN_KEY: String = "item_id"
  val JOIN_TYPE_NAME: String = "inner"

  val ROW_NUMBER: String = "row_number"
  val ROW_CONDITION: String = "row_number == 1"

  val JDBC_URL: String = "jdbc:mysql://localhost:3306/stream1"
  val JDBC_DRIVER: String = "com.mysql.cj.jdbc.Driver"

  val TABLE_NAME = "staging"
  val TABLE_NAME_FINAL = "prod"

  val USER_NAME: String = "root"
  val KEY_PASSWORD: String = "Krishna@123"

  val FAILURE_EXIT_CODE: Int = 1
  val SUCCESS_EXIT_CODE: Int = 0

}
