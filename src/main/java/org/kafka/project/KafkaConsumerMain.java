package org.kafka.project;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafka.project.model.MessageDeserializer;
import org.kafka.project.model.Person;

import java.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class KafkaConsumerMain {
    private static final String topic = "peoples_data";
    private static KafkaConsumer<String, Person> consumer;
    private static final long timeout = 3000;

    public KafkaConsumerMain() {
        consumer = getKafkaConsumer();
    }

    public Future<List<Person>> getFromOffSet(String _topic, int offSet, int num) {
        Promise<List<Person>> promise = Promise.promise();
        try {
            CopyOnWriteArrayList<Person> results = new CopyOnWriteArrayList<>();
            // subscribe consumer to our topic(s)
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(_topic);
            PartitionInfo partitionInfo = partitionInfos.stream().findFirst().orElseThrow();

            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());

            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offSet);

            long startTime = System.currentTimeMillis();
            boolean terminateRequest = false;
            while (true) {
                ConsumerRecords<String, Person> records =
                        consumer.poll(Duration.ofSeconds(1));

                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed > timeout) {
                    ArrayList<Person> people = new ArrayList<>(results);
                    terminateRequest = true;
                    promise.complete(people);
                    break;
                }

                for (ConsumerRecord<String, Person> record : records) {
                    if (results.size() == num) {
                        ArrayList<Person> people = new ArrayList<>(results);
                        terminateRequest = true;
                        promise.complete(people);
                        break;
                    }
                    results.add(record.value());
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
                if (terminateRequest) {
                    break;
                }
            }
        } catch (WakeupException e) {
            log.error("Wake up exception!");

        } catch (Exception e) {
            log.error("Exception occurred", e);
            promise.fail(e.getCause());
        }
        return promise.future();
    }

    private static KafkaConsumer<String, Person> getKafkaConsumer() {
        log.info("I am a Kafka Consumer");

        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-4";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));

        // create consumer
        KafkaConsumer<String, Person> consumer = new KafkaConsumer<>(properties);

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        return consumer;
    }
}
