package neil.demo.zappa;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>A specific exchange rate associated with a currency pair.
 * The source currency converts to the target currency at the
 * given time and at the given rate.
 * </p>
 * <p>In more realistic examples the time is more of an instant
 * in time than a date, and the exchange rate might have a
 * spread of exchange rates depending on various factors such
 * as if buying or selling, the amount, etc.
 * </p>
 */
@SuppressWarnings("serial")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CurrencyPairValue implements Serializable {
	
	@JsonDeserialize(using = LocalDateDeserializer.class)
    @JsonSerialize(using = LocalDateSerializer.class)
	private LocalDate day;
	private BigDecimal rate;
	
	/**
	 * <p>How to deserialize this class from CSV format,
	 * so "{@code 2018-11-07,1234}" becomes {@code 7th November 2018} as day and
	 * {@code 1234} as rate.
	 * format.
	 * </p>
	 */
	public static class CurrencyPairValueDeserializer implements Deserializer<CurrencyPairValue> {

		@Override
		public CurrencyPairValue deserialize(String topic, byte[] data) {
			String s = new String(data);
			String[] tokens = s.split(",");
			
			LocalDate day = LocalDate.parse(tokens[0], DateTimeFormatter.ISO_LOCAL_DATE);
			BigDecimal rate = new BigDecimal(tokens[1]);

			CurrencyPairValue currencyPairValue = new CurrencyPairValue();
			currencyPairValue.setDay(day);
			currencyPairValue.setRate(rate);

			return currencyPairValue;
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}
		@Override
		public void close() {
		}

	}
}
