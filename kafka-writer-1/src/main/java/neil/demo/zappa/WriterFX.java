package neil.demo.zappa;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Write all FX data.
 * </p>
 */
@Component
@Slf4j
public class WriterFX implements CommandLineRunner {

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	@Qualifier(MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_FX)
	private KafkaTemplate<String, String> kafkaFxProducerTemplate;

	/**
	 * <p>One single file of input, write to a Kafka topic with
	 * a fixed key.
	 * </p>
	 */
	public void run(String... args) throws Exception {
		String key = "BTCUSD";
		int partition = key.hashCode() % MyConstants.KAFKA_PARTITION_COUNT;
		List<String> priceList = this.loadBtcUsd();

		MyUtils.writeKafka(partition, key, priceList, this.kafkaFxProducerTemplate, true);
	}

	/**
	 * <p>
	 * Turn a CSV file into a list of strings, simple upload.
	 * </p>
	 */
	private List<String> loadBtcUsd() {
		String input = "classpath:btcusd.csv";
		List<String> result = new ArrayList<>();

		try {
			Resource resource = this.applicationContext.getResource(input);

			try (InputStream inputStream = resource.getInputStream();
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					if (!line.startsWith("#")) {
						result.add(line);
					}
				}
			}
		} catch (Exception e) {
			log.error("Problem reading '" + input + "'", e);
		}

		return result;
	}
}
