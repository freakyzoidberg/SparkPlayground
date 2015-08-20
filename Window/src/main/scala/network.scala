import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.streaming.kafka.KafkaUtils

case class Word(name: String, count: Int)

object Network {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Kafka Aggregate")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sqlContext = new SQLContext(ssc.sparkContext)
    val hiveContext = new HiveContext(ssc.sparkContext)

    HiveThriftServer2.startWithContext(hiveContext)
    import hiveContext.implicits._

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val people = hiveContext.createDataFrame(hiveContext.sparkContext.emptyRDD[Word])
    people.registerTempTable("windowed")

    lines.window(Minutes(1440)).foreachRDD{ rdd =>
      if (rdd.count() > 0) {
        hiveContext.read.json(rdd).toDF().registerTempTable("windowed")
      }
    }

    lines.window(Minutes(1), Minutes(1)).foreachRDD { rdd =>
      if (rdd.count() > 0) {
        hiveContext.read.json(rdd).toDF().write.mode(SaveMode.Append).parquet("persisted")
      }
    }

    lines.window(Seconds(30)).foreachRDD { (rdd, time) =>
      val all = hiveContext.sql("SELECT count(*) FROM windowed")
      println("in last 30s: " + rdd.count() + " and total is " + all.first().getLong(0))
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
