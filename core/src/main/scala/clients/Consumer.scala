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


    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Collections.singletonList("my-topic"))

    var sum: Long = 0
    var counter: Int = 0

    while(true) {
      val records: ConsumerRecords[String, String] = consumer.poll(10)
      records.asScala.foreach(record =>  {
          val value: Long = record.value().toLong
          val timeDiff: Long = System.currentTimeMillis() - value

          sum += timeDiff / 1000
          counter += 1

          if (counter >= 30) {
            println("Average latency: " + (sum / counter))
            counter = 0
            sum = 0
          }
        }
      )
    }

  }
}
