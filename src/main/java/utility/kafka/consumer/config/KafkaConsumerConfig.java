package utility.kafka.consumer.config;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import reactor.kafka.receiver.ReceiverOptions;

@Configuration
@PropertySource("classpath:/application.properties")
public class KafkaConsumerConfig {

	@Value("${kafka.broker}")
	private String broker;

	@Value("${kafka.consumer.groupId}")
	private String groupId;

	@Value("${kafka.consumer.offset.reset.earlier}")
	private String resetEarlier;

	@Value("${kafka.consumer.offset.reset.latest}")
	private String resetLatest;

	@Value("${kafka.consumer.max.poll.records}")
	private int maxPollRecords;

	@Value("${kafka.consumer.topic.name}")
	private String topicName;

	@Bean
	public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
		return new PropertySourcesPlaceholderConfigurer();
	}

	@Bean
	public Consumer<Long, String> createConsumer() {

		Properties consumerProps = new Properties();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetEarlier);
		Consumer<Long, String> consumer = new KafkaConsumer<>(consumerProps);
		consumer.subscribe(Collections.singletonList(topicName));
		return consumer;

	}

}
