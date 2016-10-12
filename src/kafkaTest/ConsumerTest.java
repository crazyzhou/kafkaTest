package kafkaTest;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ConsumerTest {
	private static Logger log = Logger.getLogger(ConsumerTest.class);
	private static final long threshold = 10000000;

	public static void main(String args[]) throws InterruptedException {
		PropertyConfigurator.configure("./conf/consumer.properties");
		Properties props = new Properties();
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 500);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList("Test"));
		long count = 0;
		try {
			log.info("startTime: " + System.currentTimeMillis());
			while (count < threshold) {
				// long startTime = System.nanoTime();
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					count++;
					if (count % 10000 == 0) {
						log.info("No. " + count + " message : " + System.currentTimeMillis());
					}
				}
			}
		} finally {
			log.info("endTime: " + System.currentTimeMillis());
			consumer.close();
		}
	}
}
