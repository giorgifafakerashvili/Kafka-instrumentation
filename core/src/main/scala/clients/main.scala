package clients

import java.util.Properties

import edu.brown.cs.systems.baggage.{Baggage, DetachedBaggage}
import edu.brown.cs.systems.pubsub.PubSub
import edu.brown.cs.systems.xtrace.XTrace
import edu.brown.cs.systems.xtrace.logging.XTraceLogger
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.types.Struct
import org.apache.kafka.common.requests.RequestHeader

import scala.collection.mutable

class MyCallback extends Callback {
  private val xtrace:XTraceLogger = XTrace.getLogger(classOf[MyCallback])

  def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit = {
    xtrace.log("Client Callback invoked")
  }
}

object Main {

  private val xtrace: XTraceLogger = XTrace.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {

    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0.asInstanceOf[Integer])
    props.put("batch.size", 16384.asInstanceOf[Integer])
    props.put("linger.ms", 1.asInstanceOf[Integer])
    props.put("buffer.memory", 33554432.asInstanceOf[Integer])
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    XTrace.startTask(true)

    xtrace.log("START")

    val myCallback:MyCallback = new MyCallback

    val producer: KafkaProducer[String, String] = new KafkaProducer(props)

    val producerRecord: ProducerRecord[String, String] = new ProducerRecord[String,String]("my-topic", "Fafakerashvili", "Fafakerashvili")


    var counter: Int = 0
    for(_ <- 1 until 2) {
      producer.send(producerRecord, myCallback)
      println(counter)
      counter = counter + 1
    }


    producer.flush()
    producer.close()

    PubSub.close()
    PubSub.join()


  }
}
