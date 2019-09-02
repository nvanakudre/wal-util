import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import utility.kafka.consumer.consumer.KafkaConsumer0;
import utility.kafka.consumer.consumer.KafkaConsumer1;
import utility.kafka.consumer.consumer.KafkaConsumer2;


@ComponentScan(basePackages = "utility.kafka.consumer")
public class UtilityConsumerApplication {

	public static void main(String... args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(UtilityConsumerApplication.class);

		KafkaConsumer0 kafkaConsumer0 = context.getBean(KafkaConsumer0.class);
		KafkaConsumer1 kafkaConsumer1 = context.getBean(KafkaConsumer1.class);
		KafkaConsumer2 kafkaConsumer2 = context.getBean(KafkaConsumer2.class);

		while (true) {
			//new Thread(() -> kafkaConsumer0.readAndProcessRecord()).start();
            kafkaConsumer0.readAndProcessRecord();
			kafkaConsumer1.readAndProcessRecord();
			kafkaConsumer2.readAndProcessRecord();

//			new Thread(() -> kafkaConsumer1.readAndProcessRecord()).start();

//			new Thread(() -> kafkaConsumer2.readAndProcessRecord()).start();
		}
	}

}
