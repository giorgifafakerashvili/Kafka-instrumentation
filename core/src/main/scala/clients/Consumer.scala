package clients

import java.util
import java.util.Properties

import edu.brown.cs.systems.pubsub.PubSub
import edu.brown.cs.systems.xtrace.XTrace
import edu.brown.cs.systems.xtrace.logging.XTraceLogger

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer


object Consumer {

  private val xtrace: XTraceLogger = XTrace.getLogger(Consumer.getClass)

  def main(Args: Array[String]): Unit = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    XTrace.startTask(true)

    xtrace.log("CONSUMER START")


    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList("my-topic"))


    while(true) {
      xtrace.log("********* POLL ****************")
      val records: ConsumerRecords[String, String] = consumer.poll(1000)
      records.asScala.foreach(record =>
        println(s"Received message: $record")
      )
      xtrace.log("*********** Records has been processed ******************")
    }



    PubSub.close()
    PubSub.join()

  }
}
