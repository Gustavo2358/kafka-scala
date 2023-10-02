import org.apache.kafka.clients.producer._

object KafkaProducerApp extends App {
  val props = new java.util.Properties()
  props.put("bootstrap.servers", "localhost:29092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[String, String](props)
  val record = new ProducerRecord[String, String]("test", "key", "value")

  while(true){
    producer.send(record).get()
    println("message sent")
    Thread.sleep(1000)
  }
  producer.close()
}
