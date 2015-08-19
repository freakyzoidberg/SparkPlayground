./bin/beeline 
Beeline version 1.4.1 by Apache Hive
beeline> !connect jdbc:hive2://127.0.0.1:10000/default
scan complete in 31ms
Connecting to jdbc:hive2://127.0.0.1:10000/default
Enter username for jdbc:hive2://127.0.0.1:10000/default: placave                                      
Enter password for jdbc:hive2://127.0.0.1:10000/default: 
log4j:WARN No appenders could be found for logger (org.apache.thrift.transport.TSaslTransport).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Connected to: Spark SQL (version 1.4.1)
Driver: Spark Project Core (version 1.4.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://127.0.0.1:10000/default> select * from tmpfinal3;                     
+-------+--------+
| name  | count  |
+-------+--------+
+-------+--------+
No rows selected (1.474 seconds)
