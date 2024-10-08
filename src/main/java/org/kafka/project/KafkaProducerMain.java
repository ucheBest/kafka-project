package org.kafka.project;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kafka.project.model.MessageSerializer;
import org.kafka.project.model.Person;

import java.util.Properties;

@Slf4j
public class KafkaProducerMain {
    private static final String topic = "people_data";
    private static final String bootstrapServers = "localhost:9092";
    private final KafkaProducer<String, Person> producer;
    private final Person[] people;

    public KafkaProducerMain() {
        this.producer = getProducer();
        this.people = DataLoader.getPeople();
    }

    public void publishAllData() {
        log.info("Publishing {} data to Kafka topic {}", this.people.length, topic);

        for (Person person : this.people) {

            ProducerRecord<String, Person> producerRecord =
                    new ProducerRecord<>(topic, person.get_id(), person);

            this.producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata. \nTopic:{}\nKey:{}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                recordMetadata.topic(), producerRecord.key(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
        }
        this.producer.flush();
        this.producer.close();
    }

    private static KafkaProducer<String, Person> getProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}

