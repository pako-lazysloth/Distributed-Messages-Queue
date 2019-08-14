import akka.actor.Actor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import Message.{StartMessage, StoreMessage}


class SendHbase extends Actor {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum",ProjectProperties.hbaseKafkaQuorum)
  conf.set("hbase.zookeeper.property.clientPort", ProjectProperties.hbaseZKclientPort)
  conf.set("hbase.cluster.distributed", ProjectProperties.hbaseClusterDistributed)

  val jobConf = new JobConf(conf) // Jobconf 必须在包 org.apache.hadoop.hbase.mapred 下
  jobConf.setOutputFormat(classOf[TableOutputFormat])  // TableOutputFormat 必须在包 org.apache.hadoop.hbase.mapred 下
  jobConf.set(TableOutputFormat.OUTPUT_TABLE, ProjectProperties.hbaseTableName)

  override def receive: Receive = {
    case  StartMessage =>
      println("INFO: SendHbase, start.....")
    case  StoreMessage(rdd: RDD[String]) =>

      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      rdd.map( array => {
          val arr = array.split(",")
          val put = new Put(Bytes.toBytes(arr(0).toInt))
          println("The arr(0) is -> " + arr(0))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
          (new ImmutableBytesWritable, put)
      }).saveAsHadoopDataset(jobConf)

    case _ =>
      println("Warning: SendHbase receiver '_' message.....")
  }

}
