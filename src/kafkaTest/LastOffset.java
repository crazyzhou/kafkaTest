package kafkaTest;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class LastOffset {

	public static void main(String args[]) throws InterruptedException {
		PropertyConfigurator.configure("./conf/consumer.properties");
		Properties props = new Properties();
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		try {
			TopicPartition partition = new TopicPartition("Location", 0);
			//long lastOffset = 346720;
			consumer.seekToEnd(Arrays.asList(partition));
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.offset());
			}
			//	consumer.seek(partition, lastOffset);
			
			// consumer.seek(partition, lastOffset + 1);
		} finally {
			consumer.close();
		}
	}
}
