package neil.demo.zappa.jet.account;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedFunctions;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.map.EntryProcessor;

import neil.demo.zappa.AccountBaseline;
import neil.demo.zappa.AccountTransaction;
import neil.demo.zappa.MyAvroDeserializer;
import neil.demo.zappa.MyConstants;
 

/**
 * <p>Read {@link AccountBaseline} and {@link AccountTransaction}
 * records into memory.
 * </p>
 * <p>For <u>simplicity</u> assume that these records are not
 * processed out of order. This is not going to be the case for
 * live so this would need handled.
 * </p>
 */
public class Account {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Pipeline build(String bootstrapServers) {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyAvroDeserializer.class.getCanonicalName());

		Pipeline pipeline = Pipeline.create();

		StreamStage<Map.Entry<String, ?>> input = 
				pipeline
				.drawFrom(KafkaSources.kafka(properties, MyConstants.KAFKA_TOPIC_NAME_ACCOUNT));

		// Baseline records write in directly with empty transaction list
		input
		.filter(entry -> AccountBaseline.class.isInstance(entry.getValue()))
		.map(oldEntry -> {
			AccountBaseline accountBaseline = (AccountBaseline) oldEntry.getValue();
			Tuple2<AccountBaseline, List<AccountTransaction>> newValue = 
					Tuple2.tuple2(accountBaseline, new ArrayList<>());
			return (Map.Entry<String, ?>) new SimpleImmutableEntry(oldEntry.getKey(), newValue);
		})
		.drainTo(
				Sinks.map(MyConstants.IMAP_NAME_ACCOUNT));

		// Delta records merge in using the provided merge function
		input
		.filter(entry -> AccountTransaction.class.isInstance(entry.getValue()))
		.drainTo((Sink<? super Entry<String, ?>>) 
				Sinks.mapWithEntryProcessor(
					MyConstants.IMAP_NAME_ACCOUNT,
					DistributedFunctions.entryKey(),
					entry -> ((EntryProcessor) new AccountMergeEntryProcessor(entry.getValue()))));

		// Log for show, compute for dough
		input
		.drainTo(Sinks.logger());

		return pipeline;
	}

}
