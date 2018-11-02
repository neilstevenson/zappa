package neil.demo.zappa;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * <p>Create some Kafka templates to simplify writing to Kafka
 * topics. We write all as "{@code <String, String>}" pairs so
 * it is clear on the topic logger what is happening. For live
 * use custom serializer classes would be better.
 * </p>
 */
@Configuration
public class MyKafkaProducerTemplates {

	@Value("${bootstrap-servers}")
	private String bootstrapServers;

	/**
	 * <p>Helper for writing "{@code <String, String>}".
	 * </p>
	 * 
	 * @param topicName To write to
	 */
	private KafkaTemplate<String, String> myStringStringTemplate(
			String topicName
			) {
        Map<String, Object> producerConfigs = new HashMap<>();

        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(
                        producerConfigs);

        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory);

        kafkaTemplate.setDefaultTopic(topicName);

        return kafkaTemplate;
	}
	
	/**
	 * <p>Kafka template for the "{@code account}" topic.
	 * </p>
	 *
	 * @return A template to use in {@link WriterAccount}
	 */
    @Bean(name = MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_ACCOUNT)
    public KafkaTemplate<String, String> kafkaAccountProducerTemplate() {
            return this.myStringStringTemplate(MyConstants.KAFKA_TOPIC_NAME_ACCOUNT);
    }

	/**
	 * <p>Kafka template for the "{@code fx}" topic.
	 * </p>
	 *
	 * @return A template to use in {@link WriterFX}
	 */
    @Bean(name = MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_FX)
    public KafkaTemplate<String, String> kafkaFxProducerTemplate() {
        return this.myStringStringTemplate(MyConstants.KAFKA_TOPIC_NAME_FX);
    }

	/**
	 * <p>Kafka template for the "{@code gps}" topic.
	 * </p>
	 *
	 * @return A template to use in {@link WriterGps}
	 */
    @Bean(name = MyConstants.BEAN_PRODUCER_PREFIX + MyConstants.KAFKA_TOPIC_NAME_GPS)
    public KafkaTemplate<String, String> kafkaGpsProducerTemplate() {
        return this.myStringStringTemplate(MyConstants.KAFKA_TOPIC_NAME_GPS);
    }
}
