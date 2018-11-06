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
 * <p>Write all GPS data.
 * </p>
 */
@Component
@Slf4j
public class WriterGPS implements CommandLineRunner {

	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	@Qualifier(MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_GPS)
	private KafkaTemplate<String, String> kafkaGpsProducerTemplate;

	/**
	 * <p>One single file of input, write to a Kafka topic with
	 * a fixed key.
	 * </p>
	 */
	public void run(String... args) throws Exception {
		String key = "18:48 Departure";
		int partition = key.hashCode() % MyConstants.KAFKA_PARTITION_COUNT;
		List<Gps> gpsList = this.loadGps();

		MyUtils.writeKafka(partition, key, gpsList, this.kafkaGpsProducerTemplate);
	}

	/**
	 * <p>
	 * Turn a CSV file into a list of GPS points.
	 * </p>
	 */
	private List<Gps> loadGps() {
		String input = "classpath:gps.csv";
		List<Gps> result = new ArrayList<>();

		try {
			Resource resource = this.applicationContext.getResource(input);

			try (InputStream inputStream = resource.getInputStream();
					InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
					BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					if (!line.startsWith("#")) {
						String[] tokens = line.split(",");

						long timestamp = Long.parseLong(tokens[0]);
						double latitude = Double.parseDouble(tokens[1]);
						double longitude = Double.parseDouble(tokens[2]);

						Gps gps = new Gps();
						gps.setTimestamp(timestamp);
						gps.setLatitude(latitude);
						gps.setLongitude(longitude);

						result.add(gps);
					}
				}
			}
		} catch (Exception e) {
			log.error("Problem reading '" + input + "'", e);
		}

		return result;
	}
}
