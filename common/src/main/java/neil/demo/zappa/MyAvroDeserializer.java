package neil.demo.zappa;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Map;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>A deserializer that can handle {@link AccountTransaction} or
 * {@link AccountBaseline} as input. These classes don't extend
 * a common base, and are passed as JSON strings.
 * </p>
 * <p>The method to determine which is a bit simplistic, try
 * to deserialize with one class and if this fails try to
 * deserialize with another.
 * </p>
 * <p>It's not good practice to mix classes on the topic in this
 * way, or to deserialize this way. It's fine for a demo, but for
 * real use a cleaner approach is in order.
 * </p>
 */
@Slf4j
public class MyAvroDeserializer implements Deserializer<Object> {

	private Schema accountBaselineSchema
		= new AccountBaseline().getSchema();
	private Schema accountTransactionSchema
		= new AccountTransaction().getSchema();
	
	@Override
	public Object deserialize(String topic, byte[] data) {
		
		try (ByteArrayInputStream byteArrayInputStream1 = new ByteArrayInputStream(data);
			 ByteArrayInputStream byteArrayInputStream2 = new ByteArrayInputStream(data);
			 DataInputStream dataInputStream1 = new DataInputStream(byteArrayInputStream1);
			 DataInputStream dataInputStream2 = new DataInputStream(byteArrayInputStream2);
				) {
			DatumReader<Object> accountBaselineReader = 
					new SpecificDatumReader<>(this.accountBaselineSchema);
			DatumReader<Object> accountTransactionReader = 
					new SpecificDatumReader<>(this.accountTransactionSchema);
			
			Decoder accountBaselineDecoder
				= DecoderFactory.get().jsonDecoder(this.accountBaselineSchema, dataInputStream1);
			Decoder accountTransactionDecoder
				= DecoderFactory.get().jsonDecoder(this.accountTransactionSchema, dataInputStream2);

			// Expecting more transactions than baselines, so try that first
			try {
				return accountTransactionReader.read(null, accountTransactionDecoder);
			} catch (AvroTypeException notAnAccountTransaction) {
				return accountBaselineReader.read(null, accountBaselineDecoder);
			}
			
		} catch (Exception e) {
			log.error(new String(data), e);
		}
		
		return null;
	}


	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
	}
	@Override
	public void close() {
	}

}
