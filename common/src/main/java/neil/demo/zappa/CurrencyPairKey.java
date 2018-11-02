package neil.demo.zappa;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>A currency pair.
 * </p>
 * <p>"{@code USD}/{@code EUR}" represents the conversion <u>from</u> (the "base")
 * US Dollars to Euros (the "quote").
 * </p>
 */
@SuppressWarnings("serial")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CurrencyPairKey implements Comparable<CurrencyPairKey>, Serializable {
	
	private String base;
	private String quote;

	// Order by source currency (base) then by destination (quote)
	@Override
	public int compareTo(CurrencyPairKey that) {
		if (!this.base.equals(that.getBase())) {
			return this.base.compareTo(that.getBase());
		} else {
			return this.quote.compareTo(that.getQuote());
		}
	}

	/**
	 * <p>How to deserialize this class from compact string format,
	 * so "{@code USDEUR}" becomes {@code US Dollars} as base and
	 * {@code Euro} as quote.
	 * format.
	 * </p>
	 */
	public static class CurrencyPairKeyDeserializer implements Deserializer<CurrencyPairKey> {

		@Override
		public CurrencyPairKey deserialize(String topic, byte[] data) {
			CurrencyPairKey currencyPairKey = new CurrencyPairKey();
			
			currencyPairKey.setBase(new String(Arrays.copyOfRange(data, 0, 3)));
			currencyPairKey.setQuote(new String(Arrays.copyOfRange(data, 3, 6)));
			
			return currencyPairKey;
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
		}
		@Override
		public void close() {
		}
		
	}
}
