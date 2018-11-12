
## Kafka Topics

    List existing topics


<code>./bin/kafka-topics.sh --zookeeper localhost:2181 --list</code>

    Describe a topic

<code>./bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic mytopic</code>

    Purge a topic

<code>./bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic mytopic --config retention.ms=1000


./bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic mytopic --delete-config retention.ms</code>

    Delete a topic 

<code>./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic mytopic</code>

    Get number of messages in a topic 
 
<code>./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mytopic --time -1 --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'</code>

    Get the earliest offset still in a topic 

<code>./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mytopic --time -2</code>

    Get the latest offset still in a topic 

<code>./bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic mytopic --time -1</code>

    Consume messages with the console consumer

<code>./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mytopic --from-beginning</code>

    Get the consumer offsets for a topic

<code>./bin/kafka-consumer-offset-checker.sh --zookeeper=localhost:2181 --topic=mytopic --group=my_consumer_group</code>

    Read from consumer_offsets

    Add the following property to  ''config/consumer.properties: exclude.internal.topics=false''

<code>./bin/kafka-console-consumer.sh --consumer.config config/consumer.properties --from-beginning --topic __consumer_offsets --zookeeper localhost:2181 --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter"</code>

### Topic Config

    set a topic's retention time

<code>./bin/kafka-configs --zookeeper localhost:2181  --entity-type topics --entity-name my-topic --alter --add-config retention.ms=128000</code>

### Kafka Consumer Groups 

    View the details of a consumer group 

<code>./bin/kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group name></code>

    Reset offset

<code>./bin/kafka-consumer-groups --bootstrap-server localhost:9092 --group <group_id> --topic <topic_name> --reset-offsets --to-earliest --execute</code>

### kafkacat

    Getting the last five message of a topic

<code>kafkacat -C -b localhost:9092 -t mytopic -p 0 -o -5 -e</code>

### Zookeeper 
    Starting the Zookeeper Shell 
 

<code>./bin/zookeeper-shell.sh localhost:2181</code>
