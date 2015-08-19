
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test





sbt packages
../../spark-1.4.1-bin-hadoop2.6/bin/spark-submit --jars spark-streaming-kafka-assembly_2.10-1.4.1.jar --master spark://Zoidbox:7077 --class Network target/scala-2.10/window-aggregate_2.10-1.2.jar 127.0.0.1 dummyGroup test 1
