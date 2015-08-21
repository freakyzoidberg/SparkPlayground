import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.JsonParser


case class Word(name: String, count: Int)

object Network {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Window Aggregate")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sqlContext = new SQLContext(ssc.sparkContext)
    val hiveContext = new HiveContext(ssc.sparkContext)

    HiveThriftServer2.startWithContext(hiveContext)
    import hiveContext.implicits._

    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


    val topicA = lines.map(line => JsonParser.parse(line)).filter { json =>
      implicit val formats = DefaultFormats
      (json \ "topic").extract[String].equalsIgnoreCase("a")
    }


    val topicA_1 = topicA.window(Seconds(1), Seconds(1))
    topicA_1.foreachRDD{ rdd =>
      println("Message count in Topic A in Last Second: " + rdd.count())
    }
    topicA_1.map { json =>
      implicit val formats = DefaultFormats
      ((json \ "messageType").extract[String], 1)
    }.reduceByKey(_ + _).foreachRDD { messageType =>
      println("Message count in Topic A Broken by Message Type Last Second: ")
      messageType.collect().foreach(println)
    }



    val topicA_10 = topicA.window(Seconds(10), Seconds(5))
    topicA_10.foreachRDD{ rdd =>
      println("Message count in Topic A in Last 10 Seconds: " + rdd.count())
    }
    topicA_10.map { json =>
      implicit val formats = DefaultFormats
      ((json \ "messageType").extract[String], 1)
    }.reduceByKey(_ + _).foreachRDD { messageType =>
      println("Message count in Topic A Broken by Message Type Last 10 Seconds: ")
      messageType.collect().foreach(println)
    }



    val topicA_10m = topicA.window(Minutes(1), Seconds(10))
    topicA_10m.foreachRDD{ rdd =>
      println("Message count in Topic A in last Minute: " + rdd.count())
    }
    topicA_10m.map { json =>
      implicit val formats = DefaultFormats
      ((json \ "messageType").extract[String], 1)
    }.reduceByKey(_ + _).foreachRDD { messageType =>
      println("Message count in Topic A Broken by Message Type Last Minute: ")
      messageType.collect().foreach(println)
    }







//    lines.window(Seconds(1)).foreachRDD{ rdd =>
//      if (rdd.count() > 0) {
//        hiveContext.read.json(rdd).toDF().first().get()
//        hiveContext.read.json(rdd).toDF().registerTempTable("windowed")
//        println(hiveContext.sql("select count(*) from windowed group by a"))
//      }
//    }
//
//    lines.window(Seconds(5), Seconds(5)).foreachRDD { rdd =>
//      if (rdd.count() > 0) {
//        hiveContext.read.json(rdd).toDF().write.mode(SaveMode.Append).parquet("persisted")
//        hiveContext.sql("CREATE TABLE IF NOT EXISTS persisted USING org.apache.spark.sql.parquet OPTIONS ( path '/Users/zoidberg/Documents/Spark/spark-1.4.1-bin-hadoop2.6/SparkPlayground/Window/persisted')")
//        hiveContext.sql("REFRESH TABLE persisted")
//      }
//    }

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
}
