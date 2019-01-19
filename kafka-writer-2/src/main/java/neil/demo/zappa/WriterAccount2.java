package neil.demo.zappa;

import java.util.List;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * <p>Write the additional account data.
 * </p>
 */
@Component
public class WriterAccount2 implements CommandLineRunner {

	@Autowired
	@Qualifier(MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_ACCOUNT)
	private KafkaTemplate<String, String> kafkaAccountProducerTemplate;

	/**
	 * <p>Simple iteration, print all data for each key, even if this results in
	 * transactions being written out of date order.
	 * </p>
	 */
	@Override
	public void run(String... args) throws Exception {
		TreeMap<String, List<AccountTransaction>> transactions
			= MyUtils.loadTransactions(TestData.ACCOUNT_TRANSACTIONS_2);
		
		for (String key : transactions.keySet()) {
			int partition = key.hashCode() % MyConstants.KAFKA_PARTITION_COUNT;
			List<AccountTransaction> accountTransactionList = transactions.get(key);

			MyUtils.writeKafka(partition, key, accountTransactionList, this.kafkaAccountProducerTemplate, false);
		}
	}

}
