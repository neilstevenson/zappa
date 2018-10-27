package neil.demo.zappa;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * <p>Write the initial account data.
 * </p>
 */
@Component
public class WriterAccount1 implements CommandLineRunner {

	@Autowired
	@Qualifier(MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_ACCOUNT)
	private KafkaTemplate<String, String> kafkaAccountProducerTemplate;

	
	/**
	 * <p>Write the base record, then all the subsequent transactions.
	 * </p>
	 * <p.Unnecessearily complicated output. Instead of doing it the easy way,
	 * printing all per account (as {@link WriterAccount2} does), try to interleave
	 * the output printing.
	 * </p>
	 */
	@Override
	public void run(String... args) throws Exception {

		// Write all the baselines
		TreeMap<String, AccountBaseline> baselines
			= MyUtils.loadBaselines(TestData.ACCOUNT_BASELINE);
		
		for (String key : baselines.keySet()) {
			int partition = key.hashCode() % MyConstants.KAFKA_PARTITION_COUNT;
			AccountBaseline value = baselines.get(key);
			
			List<AccountBaseline> accountBaselineList = Collections.singletonList(value);

			MyUtils.writeKafka(partition, key, accountBaselineList, this.kafkaAccountProducerTemplate);
		}
		
		// Write all the transactions
		TreeMap<String, List<AccountTransaction>> transactions
			= MyUtils.loadTransactions(TestData.ACCOUNT_TRANSACTIONS_1);

		List<String> keys = new ArrayList<>(transactions.keySet());

		// How many keys, longest value string
		int maxKey = keys.size();
		int maxValue = 0;
		for (Map.Entry<String, List<AccountTransaction>> entry : transactions.entrySet()) {
			if (entry.getValue().size() > maxValue) {
				maxValue = entry.getValue().size();
			}
		}

		// Iterate across values and within that per key
		for (int valIndex = 0 ; valIndex < maxValue ; valIndex++) {
			for (int keyIndex = 0 ; keyIndex < maxKey ; keyIndex++) {
				String key = keys.get(keyIndex);
				List<AccountTransaction> values = transactions.get(key);
				if (values.size() > valIndex) {
					int partition = key.hashCode() % MyConstants.KAFKA_PARTITION_COUNT;
					AccountTransaction transaction = values.get(valIndex);
					List<AccountTransaction> accountTransactionList = Collections.singletonList(transaction);

					MyUtils.writeKafka(partition, key, accountTransactionList, this.kafkaAccountProducerTemplate);
				}
			}
		}
	}

}
