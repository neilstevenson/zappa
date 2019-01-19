package neil.demo.zappa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Shared stuff, Kafka writing etc
 * </p>
 */
@Slf4j
public class MyUtils {

	/**
	 * <p>Write something to a Kafka topic, counting if
	 * successful or not.
	 * </p>
	 * 
	 * @param partition
	 * @param key
	 * @param value
	 * @param kafkaProducerTemplate
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static void writeKafka(int partition, Object key, Collection values,
			KafkaTemplate kafkaProducerTemplate, boolean delay
			) throws Exception {
		AtomicLong onFailureCount = new AtomicLong(0);
		AtomicLong onSuccessCount = new AtomicLong(0);
		CountDownLatch countDownLatch = new CountDownLatch(values.size());

		for (Object value : values) {
			if (delay) {
				try {
					TimeUnit.MILLISECONDS.sleep(50L);
				} catch (Exception ignored) {
					;
				}
			}
			
			ListenableFuture<SendResult<String, String>> sendResult =
					kafkaProducerTemplate.sendDefault(partition, key.toString(), value.toString());

			sendResult.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
				@Override
				public void onSuccess(SendResult<String, String> sendResult) {
					onSuccessCount.incrementAndGet();
					ProducerRecord<String, String> producerRecord = sendResult.getProducerRecord();
					countDownLatch.countDown();
					log.trace("wrote '{}'=='{}'", producerRecord.key(), producerRecord.value());
				}

				@Override
				public void onFailure(Throwable t) {
					onFailureCount.incrementAndGet();
					countDownLatch.countDown();
					log.error("onFailure()", t);
				}
			});
		}

		// Await callbacks confirming all writes happened, successfully or not
		countDownLatch.await();

		if (onFailureCount.get() > 0) {
			throw new RuntimeException(onFailureCount.get() + " failures writing to Kafka");
		} else {
			log.info("Wrote {} record{} to topic '{}'", 
					onSuccessCount.get(), (onSuccessCount.get() == 1 ? "" : "s"),
					kafkaProducerTemplate.getDefaultTopic());
		}
		
	}
	
	
	/**
	 * <p>Turn testdata into something better for outputting, for transactions
	 * </p>
	 *
	 * @param transactions From {@link TestData}
	 * @return Per account, in sequence
	 */
	protected static TreeMap<String, List<AccountTransaction>> loadTransactions(String[][] transactions) {
		TreeMap<String, List<AccountTransaction>> result = new TreeMap<>();
		
		for (String[] transaction : transactions) {
			String key = transaction[0];
			Double amount = Double.parseDouble(transaction[2]);

			List<AccountTransaction> value = result.getOrDefault(key, new ArrayList<>());
			
			AccountTransaction accountTransaction = new AccountTransaction();
			accountTransaction.setWhen(transaction[1]);
			accountTransaction.setAmount(Math.abs(amount));
			accountTransaction.setDebit(amount < 0);
			accountTransaction.setDescription(transaction[3]);
			
			value.add(accountTransaction);
			
			result.put(key, value);
		}
		
		return result;
	}


	/**
	 * <p>Turn testdata into something better for outputting, for transactions
	 * </p>
	 * 
	 * @param accountBaseline from {@link TestData}
	 * @return One per account
	 */
	public static TreeMap<String, AccountBaseline> loadBaselines(String[][] accountBaseline) {
		TreeMap<String, AccountBaseline> result = new TreeMap<>();
		
		for (String[] baseline : accountBaseline) {
			String key = baseline[0];
			
			AccountBaseline value = new AccountBaseline();
			value.setBalance(Double.parseDouble(baseline[3]));
			value.setOwner(baseline[1]);
			value.setWhen(baseline[2]);
			
			result.put(key, value);
		}
		
		return result;
	}

}
