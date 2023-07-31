import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger._
import org.apache.spark.sql.functions.{col, from_json, from_csv}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//This stream will print the output to File (DFS) ( Kafka -> File)
object KafkaConsumer2 {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Kafkaprocon")
      .master("local[1]")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._

    // Subscribe to 1 topic
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafkastream")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
      .writeStream
      .outputMode("append") // default
      .format("orc")
      .option("path", "data/output/Clickstream")
      .option("checkpointLocation", "FQDN")
      .trigger(ProcessingTime("30 seconds")) // only change in query
      .start()
      .awaitTermination()

  }
}