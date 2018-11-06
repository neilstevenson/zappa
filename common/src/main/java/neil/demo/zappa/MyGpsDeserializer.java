package neil.demo.zappa;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>A deserializer that can handle {@link Gps} from JSON.
 * </p>
 */
@Slf4j
public class MyGpsDeserializer implements Deserializer<Gps> {

	private Schema gpsSchema
		= new Gps().getSchema();
	
	@Override
	public Gps deserialize(String topic, byte[] data) {
		
		try (ByteArrayInputStream byteArrayInputStream1 = new ByteArrayInputStream(data);
			 DataInputStream dataInputStream1 = new DataInputStream(byteArrayInputStream1);
				) {
			DatumReader<Gps> gpsReader = 
					new SpecificDatumReader<>(this.gpsSchema);
			
			Decoder gpsDecoder
				= DecoderFactory.get().jsonDecoder(this.gpsSchema, dataInputStream1);

			return gpsReader.read(null, gpsDecoder);
			
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
