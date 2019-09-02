package utility.kafka.consumer.consumer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import utility.kafka.consumer.config.KafkaConsumerConfig;
import utility.kafka.consumer.db.ConsumerDBOperation;


@Service
public class KafkaConsumer2 {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer2.class);
    @Value("${kafka.consumer.topic.name}")
    private String topicName;

    @Autowired
    KafkaConsumerConfig kafkaConsumerConfig;

    @Autowired
    ConsumerDBOperation consumerDBOperation;

    public void readAndProcessRecord() {
        //.out.println("Inside KafkaConsumer0");
        Consumer<Long, String> consumer = kafkaConsumerConfig.createConsumer();
        boolean flag = true;
        while (flag) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
                try {
                    consumerDBOperation.insertWALRecord(String.valueOf(record.value()));
                } catch (SQLException | IOException e) {
                    e.printStackTrace();
                }
                consumer.commitAsync();
                log.info("Record processed");
            });
        }
    }
}
