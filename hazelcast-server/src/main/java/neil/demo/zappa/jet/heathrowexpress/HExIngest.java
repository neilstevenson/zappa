package neil.demo.zappa.jet.heathrowexpress;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;

import neil.demo.zappa.MyConstants;
import neil.demo.zappa.MyGpsDeserializer;

/**
 * <p>Upload positions for the Heathrow Express
 * </p>
 */
public class HExIngest {
	
	private static final String PREFIX = HExIngest.class.getSimpleName() + "::";

	public static Pipeline build(String bootstrapServers) {
	
		Properties properties = new Properties();
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyGpsDeserializer.class.getCanonicalName());

		
		/* A source directly and two sinks, the logger is optional
		 */
		Pipeline pipeline = Pipeline.create();

        StreamStage<Entry<Object, Object>> source = 
                        pipeline.drawFrom(KafkaSources.kafka(properties, MyConstants.KAFKA_TOPIC_NAME_GPS))
                        .setName("kafkaSource");
        
        source.drainTo(Sinks.map(MyConstants.IMAP_NAME_POSITION)).setName("mapSink");
        source.drainTo(Sinks.logger(o -> new String(PREFIX + o))).setName("logSink");
        
        return pipeline;
	}
}
