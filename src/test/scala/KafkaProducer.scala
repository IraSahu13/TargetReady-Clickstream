import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaCsvProducer {
  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Enter your own bootstrap server IP:PORT
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    val fileName = "data/input/clickstream/clickstream_log.csv"

    val topicName = "kafkastream"

    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
      // Extract Key
      val key = line.split(",") {
        0
      }

      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)
      // println(key)
      // Send to topic
      producer.send(record)
    }

    producer.flush()
    producer.close()
  }
}