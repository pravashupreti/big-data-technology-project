/bin/kafka-topics --list --bootstrap-server localhost:9092
/bin/kafka-topics --create --bootstrap-server localhost:9092  --partitions 3 --replication-factor 1 --topic test-topic
/bin/kafka-topics --describe --bootstrap-server localhost:9092  test-topic
/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic
/bin/kafka-topics --delete --bootstrap-server localhost:9092 --topic test-topic