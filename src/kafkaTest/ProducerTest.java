package kafkaTest;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ProducerTest {

	private static Logger log = Logger.getLogger(ProducerTest.class);
	private static final long threshold = 10000000;

	private static String init(int n) {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < n; i++)
			s.append('a');
		return s.toString();
	}

	public static void main(String args[]) throws InterruptedException {
		PropertyConfigurator.configure("./conf/producer.properties");
		Properties props = new Properties();
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		String s_200b = init(200);
		String s_500b = init(500);
		String s_1kb = init(1024);
		String s_2kb = init(2048);
		String s_5kb = init(5120);
		String s_10kb = init(10240);
		long count = 0;
		try {
			log.info("startTime: " + System.currentTimeMillis());
			while (count < threshold) {
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(args[0], s_1kb);
				//long startTime = System.nanoTime();
				producer.send(record);
				count++;
				if (count % 10000 == 0) {
					log.info("No. " + count + " message : " + System.currentTimeMillis());
				}
			}
		} finally {
			log.info("endTime: " + System.currentTimeMillis());
			producer.close();
		}
	}
}
