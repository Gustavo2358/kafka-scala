import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util
import java.util._

object KafkaConsumerApp extends App {
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "new")

  val admin = AdminClient.create(props)
  val topic = new NewTopic("test", 1, 1.toShort)
  val result = admin.createTopics(Collections.singletonList(topic))

  result.all().get()
  println("Topic created")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Arrays.asList("test"))

  while (true) {
    val records = consumer.poll(100)
    records.forEach(record => {
      println(s"Consumed record with key ${record.key} and value ${record.value}")
    })
  }
}