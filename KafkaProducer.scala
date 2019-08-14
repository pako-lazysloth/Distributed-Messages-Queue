import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import java.util.Properties

import Message.{SendMessage, StartMessage, StopMessage}
import akka.actor._
import kafka.javaapi.producer.Producer

class KafkaProducer extends Actor {

  private var producer: Producer[Integer, String] = null
  private val props = new Properties
  private var num = 0

  props.put("bootstrap.servers", ProjectProperties.kafkaServers)
  props.put("serializer.class", ProjectProperties.kafkaserializer)
  props.put("metadata.broker.list", ProjectProperties.kafkaBrokers)

  producer = new Producer[Integer, String](new ProducerConfig(props)) // 创建 可以向 Kafka 发送消息的发送器

  override def receive: Receive = {
  case StartMessage =>
    println("INFO: KafkaProducer, start.....")
  case SendMessage =>
    println("INFO: Requirement Receive.....")
    val message = new String( num + "," + "Mike" + "," + (num * 100) ) // 消息的格式！
    num += 1 // 更新消息参数

    ProjectProperties.kafkaTopics.foreach(topic => // 将消息发送给每一个 Topic 对应的 Kafka 存储区域
      producer.send(new KeyedMessage[Integer, String](topic, message))
    )

    println("INFO: SendMessage function correctly.....")
  case StopMessage =>
    println("INFO: KafkaProducer,stopped!")
    context.stop(self)
  case _ =>
    println("Warning: KafkaProducer receiver '_' message.....")

}
}
