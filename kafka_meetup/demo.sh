# pipeline_kafka Broker API
rm -rf /tmp/zookeeper; ./bin/zookeeper-server-start.sh config/zookeeper.properties
rm -rf /tmp/kafka-logs; ./bin/kafka-server-start.sh config/server.properties

# pipeline_kafka Consumer API
cat ~/snippets/pipeline_kafka.conf >> ~/pdb/data/pipelinedb.conf 
./bin/kafka-topics.sh --zookeeper localhost:2181 --topic consumer_topic --create --partitions 5 --replication-factor 1
for i in $(seq 1 1000); do echo { \"x\": $i }; done | kafkacat -P -b localhost:9092 -t my_topic 

# pipeline_kafka Producer API
./bin/kafka-topics.sh --zookeeper localhost:2181 --topic producer_topic --create --partitions 1 --replication-factor 1
kafkacat -C -b localhost:9092 -t producer_topic

# pipeline_kafka Demo
./bin/kafka-topics.sh --zookeeper localhost:2181 --topic ppg_topic --create --partitions 1 --replication-factor 1
./bin/kafka-topics.sh --zookeeper localhost:2181 --topic mvp_topic --create --partitions 1 --replication-factor 1
for i in $(seq 1 100); do echo { \"id\": $(($RANDOM % 8)), \"points\": $(($RANDOM % 30)) }; done | kafkacat -P -b localhost:9092 -t ppg_topic 
kafkacat -C -b localhost:9092 -t mvp_topic
