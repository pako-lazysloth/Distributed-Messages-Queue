object ProjectProperties {
  val kafkaServers: String = "localhost:9092"
  val kafkaBrokers: String = "localhost:9092"
  val kafkaTopics = Set("Kafka_Spark_Hbase")
  val kafkaGroupId: String = "group"
  val kafkaZKConnecter: String = "localhost:2182"
  val kafkaserializer: String = "kafka.serializer.StringEncoder"
  val kafkaAutoOffsetReset: String = "smallest" // 'smallest' or 'largest'

  val sparkAppName: String = "Kafka-Spark-Hbase"
  val sparkMaster: String = "local[2]" // local[n],其中 n 表示使用几核 // !!! 为了通过，暂时限定为 2 核， 本来是 local[*]
  val sparkMaxRatePerPartition: String = "100"
  val sparkSerializer: String =  "org.apache.spark.serializer.KryoSerializer"

  val hbaseKafkaQuorum: String = "localhost"
  val hbaseZKclientPort: String = "2182"
  val hbaseClusterDistributed: String = "false"
  val hbaseTableName: String = "MyTable"
}

abstract class ProjectProperties {
  val kafkaServers: String
  val kafkaBrokers: String
  val kafkaTopics: String
  val kafkaGroupId: String
  val kafkaZKConnecter: String
  val kafkaserializer: String
  val kafkaAutoOffsetReset: String

  val sparkAppName: String
  val sparkMaster: String
  val sparkMaxRatePerPartition: String
  val sparkSerializer: String

  val hbaseKafkaQuorum: String
  val hbaseZKclientPort: String
  val hbaseClusterDistributed: String
  val hbaseTableName: String
}
