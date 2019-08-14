/*
Sloth: The main message, to start everything.
 */
import Message.{SendMessage, StartMessage, StopMessage, StoreMessage}
import akka.actor._
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.Cluster
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object Project {
  def main(args:Array[String]) {

    //日志系统的初始化和配置
    BasicConfigurator.configure() // 自动快速地使用缺省Log4j环境, 为日志提供引导设置
    Logger.getLogger("org").setLevel(Level.WARN) // 也是日志配置，但与上面不是同一日志系统

    // 初始化 SparkContext 和 StreamingContext
    val sparkConf = new SparkConf().setAppName(ProjectProperties.sparkAppName)
    sparkConf.setMaster(ProjectProperties.sparkMaster)
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", ProjectProperties.sparkMaxRatePerPartition)
    sparkConf.set("spark.serializer", ProjectProperties.sparkSerializer)

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2)) // 利用 SparkContext 初始化 StreamingContext

    // 设置 Kafka 参数
    val kafkaParams = HashMap[String, String](
      "metadata.broker.list" -> ProjectProperties.kafkaBrokers,
      "group.id" -> ProjectProperties.kafkaGroupId,
      "zookeeper.connect" -> ProjectProperties.kafkaZKConnecter, // Sloth: Temporarily join
      "auto.offset.reset" -> ProjectProperties.kafkaAutoOffsetReset
    )

    // 启动 Akka 多线程， 创建 Akka 系统的所有角色
    val system = ActorSystem("MainSystem") // 创建 Akka 主系统,服务于 Kakfa 消息的生产著.
    val kafkaProducer = system.actorOf(Props[KafkaProducer],name="kafkaProducer")  // 创建 KafkaProducer 的 Akka 类
    val sendHbase = system.actorOf(Props[SendHbase],name="sendHbase") // 创建 SendHbase 的 Akka 类!!


    //实例化 SparkConsumer 类，启动 SparkStreaming 多线程，实现 Kafka -> SparkStreaming 的能力
    val kc = new Cluster(kafkaParams)
    val sparkConsumer = new SparkConsumer

    sparkConsumer.setOrUpdateOffsets(kc) // 初始化对应 Topic 和 Partitions 的 offsets.
    val messages = sparkConsumer.createDirectStream(ssc, kc, kafkaParams) // 从 Kafka 中 提取消息, 返回 message 作为消息载体

    messages.foreachRDD(rdd => {

      if (rdd.isEmpty()) {
          println("INFO -> The RDD is Empty..... ")
      } else {

        // 处理消息的逻辑
        println("INFO: SparkConsumer | Show the received messages.....")
        sendHbase ! new StoreMessage(rdd.values) // 为了实验而注释

        sparkConsumer.updateZKOffsets(rdd, kc) // 将 rdd 的 offsets 与 zookeeper 的同步

      }

    })
    ssc.start()

    // 产生消息的逻辑
    println("INFO: Starting the whole project.....")
    kafkaProducer ! StartMessage // 主程序向 KafkaProducer 发送启动消息
    sendHbase ! StartMessage

    println("INFO: KafkaProducer | Starting to send the messages.....")
    for(i <- 0 to 1000) {
      kafkaProducer ! SendMessage // 主程序向 KafkaProducer 发送要求发送消息的消息
    }
    kafkaProducer ! StopMessage // 主程序向 KakfaProducer 发送注销消息

    ssc.awaitTermination()  // awaitTermination 一定不能放在前面

    sc.stop()
  }

}
