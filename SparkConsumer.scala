import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{Cluster, HasOffsetRanges, KafkaUtils}

import scala.collection.immutable.HashMap

class SparkConsumer{

  // 对 Topic 对应 Zookeeper 的 offset 进行初始化, 目的是将 消息是否被消费 的两种情况的 offsets 进行统一, 为 createDirectStream 提供统一接口
  def setOrUpdateOffsets(kc: Cluster): Unit = {

    ProjectProperties.kafkaTopics.foreach(topic => {

      println("INFO: setOrUpdateOffsets....., current topic:" + topic)
      var hasConsumed = true // hasConsumed 默认为消费过
      val kafkaPartitionsE = kc.get.getPartitions(Set(topic)) // 从 Cluster 中得到目前 topic 对应的 Partitions
      if (kafkaPartitionsE.isLeft) throw new SparkException("INFO: Get kafka partition failed.....:") // 如果 Partition 报错, 说明该 Topic 不存在, 直接终止程序
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.get.getConsumerOffsets(ProjectProperties.kafkaGroupId, kafkaPartitions) // 从 Topic 和 Partitions 获得 consumer 对应的 offsets 集
      if (consumerOffsetsE.isLeft) hasConsumed = false // 如果 offsets 不存在, 则说明该 Topic 根本没有被消费过
      if (hasConsumed) {

        //如果有消费过，有两种可能，如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
        //针对这种情况，只要判断一下zk上的consumerOffsets和leaderEarliestOffsets的大小，如果consumerOffsets比leaderEarliestOffsets还小的话，说明是过时的offsets,这时把leaderEarliestOffsets更新为consumerOffsets
        val leaderEarliestOffsets = kc.get.getEarliestLeaderOffsets(kafkaPartitions).right.get //将 Kafka 上存储得最早的 offset 挑出来
        println(leaderEarliestOffsets)
        val consumerOffsets = consumerOffsetsE.right.get
        // 请多注意这里的逻辑
        val flag = consumerOffsets.forall {
          case (tp, n) => n < leaderEarliestOffsets(tp).offset // 如果 kafka 上保存的最早 offset 还比 consumer 的 offsets 要晚, 那么 offset 为 true.
        }
        if (flag) {   // 如果确实 consumer 的 offsets 都过时了
          println("INFO: consumer group | " + ProjectProperties.kafkaGroupId + " | offsets已经过时，更新为leaderEarliestOffsets.....")
          val offsets = leaderEarliestOffsets.map {
            case (tp, offset) => (tp, offset.offset) // 校正 consumer 的 offset 的偏移值
          }
          kc.get.setConsumerOffsets(ProjectProperties.kafkaGroupId, offsets) // 更新 Kafka 的 offsets 值
        }
        else { // 否则, consumer 的 offsets 不算过时
          println("INFO: consumer group | " + ProjectProperties.kafkaGroupId + " | offsets正常，无需更新.....")
        }
      }
      else { // 否则, 说明 Consumer 没有消费过这个 Topic
        //如果没有被消费过，则从最新的offset开始消费。
        val leaderLatestOffsets = kc.get.getLatestLeaderOffsets(kafkaPartitions).right.get // 将 zookeeper 上最新的 offset 作为 consumer 的 offsets
        println(leaderLatestOffsets)
        println("INFO: consumer group | " + ProjectProperties.kafkaGroupId + " | 还未消费过，更新为leaderLatestOffsets")
        val offsets = leaderLatestOffsets.map { // 同样校正 offset 的值
          case (tp, offset) => (tp, offset.offset)
        }
        kc.get.setConsumerOffsets(ProjectProperties.kafkaGroupId, offsets) // 将 consumer offset 结果保存在 zookeeper 上
      }
    })
  }

  // 将 CreateDirectStream 方法封装, 提高了安全性
  def createDirectStream(ssc: StreamingContext,kc: Cluster, kafkaParams: HashMap[String, String]): InputDStream[(String, String)] = {

    //val extractors = streamingConfig.getExtractors()
    //从zookeeper上读取offset开始消费message
    val message = {
      val kafkaPartitionsE = kc.get.getPartitions(ProjectProperties.kafkaTopics)
      if (kafkaPartitionsE.isLeft) throw new SparkException("INFO: Kafka partition failed.....:")
      val kafkaPartitions = kafkaPartitionsE.right.get
      val consumerOffsetsE = kc.get.getConsumerOffsets(ProjectProperties.kafkaGroupId, kafkaPartitions)
      if (consumerOffsetsE.isLeft) throw new SparkException("INFO: kafka consumer offsets failed......:")
      val consumerOffsets = consumerOffsetsE.right.get
      consumerOffsets.foreach { // 输出 sonsumer 保存在 Kafka 上的 offset 值
        case (tp, n) => println("===================================" + tp.topic + "," + tp.partition + "," + n)
      }
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)]( // 从 Kafka 上 拉取消息, 并将格式从 RDD 转换为 InputDStream 类型
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }
    message // 返回 message, message 类型为 InputDStream

  }

  // 在 Consumer 消费后同步 Consumer 自身和存储在 kafka 上的 offsets
  def updateZKOffsets(rdd: RDD[(String, String)], kc: Cluster): Unit = {
    println("INFO: Call Function 'updateZKOffsets'.....")
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 提取 Consumer 本地 offsets

    for (offsets <- offsetsList) { // 将本地的 offset 同步到 Kafka 上
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      val o = kc.get.setConsumerOffsets(ProjectProperties.kafkaGroupId, Map((topicAndPartition, offsets.untilOffset)))
      if (o.isLeft) {
        println(s"INFO: Error updating the offset to Kafka cluster.....: ${o.left.get}")
      }
    }
  }

}


