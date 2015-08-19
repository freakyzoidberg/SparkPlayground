nc -lk 9999 

sbt package
../../spark-1.4.1-bin-hadoop2.6/bin/spark-submit --master spark://Zoidbox:7077 --class Network target/scala-2.10/network-project_2.10-1.2.jar