package com.targetReady.data.pipeline.variables

import org.apache.spark.sql.SparkSession

object VariableDeclarations {
//  Spark Session Configurations
   val SERVER_ID : String= "localhost:9092"
   val APP_NAME : String= "TargetReady-Team2-ClickstreamDataPipeline"
   val MASTER_SERVER : String= "local[*]"


//   Kafka Topics
   val ITEM_DATA_TOPIC : String = "itemDataTest"
   val CLICKSTREAM_DATA_TOPIC : String = "clickstreamDataTest"


//   Paths
   val CLICKSTREAM_DATA_FILE_PATH : String= "data/input/clickstream/clickstream_log.csv"
   val ITEM_DATA_FILE_PATH : String= "data/input/item/item_data.csv"
   val NULL_VALUE_PATH : String= "data/output/pipelineFailures/NullValues"
   val OUTPUT_PATH = "data/output"
   val CHECKPOINT_PATH : String = "checkpoint"

//  File format
   val READ_FORMAT : String= "csv"
   val WRITE_FORMAT : String= "orc"
   val NULL_VALUE_FILE_FORMAT : String = "csv"


//  Columns in Item Data
   val ITEM_ID: String = "item_id"
   val DEPARTMENT_NAME: String = "department_name"
   val PRODUCT_TYPE: String = "product_type"
   val ITEM_PRICE: String = "item_price"
   val COLUMN_NAMES_ITEM_DATA: List[String] = List(ITEM_ID, ITEM_PRICE, PRODUCT_TYPE, DEPARTMENT_NAME)
   val COLUMNS_CHECK_NULL_DQ_CHECK_ITEM_DATA: Seq[String] = Seq(VariableDeclarations.ITEM_ID)


//  Columns in Clickstream Data
   val ID : String="id"
   val EVENT_TIMESTAMP : String= "event_timestamp"
   val SESSION_ID : String= "session_id"
   val VISITOR_ID : String= "visitor_id"
   val DEVICE_TYPE : String="device_type"
   val REDIRECTION_SOURCE : String= "redirection_source"
   val VALUE : String = "value"
   val COLUMN_NAMES_CLICKSTREAM_DATA: Seq[String] = Seq(VariableDeclarations.ID, VariableDeclarations.EVENT_TIMESTAMP, VariableDeclarations.DEVICE_TYPE, VariableDeclarations.SESSION_ID, VariableDeclarations.VISITOR_ID, VariableDeclarations.ITEM_ID, VariableDeclarations.REDIRECTION_SOURCE)
   val COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA: Seq[String] = Seq(VariableDeclarations.SESSION_ID, VariableDeclarations.ID, VariableDeclarations.VISITOR_ID)


//  Primary Keys
   val PRIMARY_KEY_CLICKSTREAM : Seq[String] = Seq(VariableDeclarations.SESSION_ID, VariableDeclarations.ITEM_ID)
   val PRIMARY_KEY_ITEM_DATA : Seq[String] = Seq(VariableDeclarations.ITEM_ID)


//  Timestamp datatype and timestamp format for changing datatype
   val TIMESTAMP_DATATYPE : String = "timestamp"
   val TIMESTAMP_FORMAT : String = "MM/dd/yyyy H:mm"


//  Columns for changing data type
   val COLUMNS_VALID_DATATYPE_CLICKSTREAM : Seq[String] = Seq(VariableDeclarations.EVENT_TIMESTAMP)
   val COLUMNS_VALID_DATATYPE_ITEM : Seq[String] = Seq(VariableDeclarations.ITEM_PRICE)


//   New Data Type
   val NEW_DATATYPE_CLICKSTREAM : Seq[String] = Seq("timestamp")
   val NEW_DATATYPE_ITEM : Seq[String] = Seq("float")


//   Join conditions
   val JOIN_KEY : String = "item_id"
   val JOIN_TYPE_NAME : String = "inner"
   val ROW_NUMBER : String = "row_number"
   val ROW_CONDITION : String = "row_number == 1"


//   MYSQL Server Configurations
   val MYSQL_DB_NAME : String="test_schema"
   val MYSQL_SERVER_PORT : String="3306"
   val MYSQL_SERVER_IP : String="localhost:"
   val TABLE_NAME : String= "clicktreamDF1"
   val TABLE_NAME_FINAL : String= "clicktreamDF2"
   val USER_NAME: String = "root"
   val KEY_PASSWORD: String = "Ira@123"


//   JDBC configurations
   val JDBC_URL: String = "jdbc:mysql://localhost:3306/test_schema"
//   + MYSQL_SERVER_IP + MYSQL_SERVER_PORT + "/" + MYSQL_DB_NAME
   val JDBC_DRIVER: String = "com.mysql.cj.jdbc.Driver"


//   Pipeline Exit Codes
   val FAILURE_EXIT_CODE: Int = 1
   val SUCCESS_EXIT_CODE: Int = 0
}
