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
    val ssc = new StreamingContext(conf, Seconds(30))
    val sqlContext = new SQLContext(ssc.sparkContext)
    val hiveContext = new HiveContext(ssc.sparkContext)

    HiveThriftServer2.startWithContext(hiveContext)
    import hiveContext.implicits._

    // Split each line into words
    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val events = pairs.reduceByKey(_ + _)

    val people = hiveContext.createDataFrame(hiveContext.sparkContext.emptyRDD[Word])
    people.registerTempTable("windowed")

    events.map(p => Word(p._1, p._2)).window(Minutes(1000)).foreachRDD { rdd =>
      if (rdd.count() > 0) {
        rdd.toDF().cache().registerTempTable("windowed")
      }
    }

    events.map(p => Word(p._1, p._2)).window(Seconds(30)).foreachRDD { (rdd, time) =>
      val all = hiveContext.sql("SELECT count(*) FROM windowed")
      println("in last 30s: " + rdd.count() + " and total is " + all.first().getLong(0))
    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}