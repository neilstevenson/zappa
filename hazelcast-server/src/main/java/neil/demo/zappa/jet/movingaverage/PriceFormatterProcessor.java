package neil.demo.zappa.jet.movingaverage;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import com.hazelcast.jet.core.AbstractProcessor;

import neil.demo.zappa.MyConstants;
import neil.demo.zappa.TimePrice;

/**
 * <p>A processor to do some simple refomatting of input value
 * to make it into a map entry.
 * </p>
 * <p>Input from ordinal 0 is the current price, handled by
 * {@link PriceFormatterProcessor#tryProcess0}. Ordinal 1
 * is the average of 50, handled by {@link PriceFormatterProcessor#tryProcess1}.
 * Ordinal 2 is the average of 200, handled by
 * {@link PriceFormatterProcessor#tryProcess2}.
 * </p>
 * <p>All three input ordinals could have been handled by
 * {@link AbstractProcessor#tryProcess(int, Object)} as an alternative.
 * </p>
 */
public class PriceFormatterProcessor extends AbstractProcessor {

	/**
	 * <p>Format incoming price from ordinal 0
	 */
	@Override
	protected boolean tryProcess0(Object item) {
		TimePrice timePrice = (TimePrice) item;
		
		Entry<String, TimePrice> outputEntry =
				new SimpleImmutableEntry<>(MyConstants.KEY_CURRENT, timePrice);
		
		return super.tryEmit(outputEntry);
	}

	/**
	 * <p>Format incoming price from ordinal 1
	 */
	@Override
	protected boolean tryProcess1(Object item) {
		TimePrice timePrice = (TimePrice) item;
		
		Entry<String, TimePrice> outputEntry =
				new SimpleImmutableEntry<>(MyConstants.KEY_50_POINT, timePrice);
		
		return super.tryEmit(outputEntry);
	}

	/**
	 * <p>Format incoming price from ordinal 2
	 */
	@Override
	protected boolean tryProcess2(Object item) {
		TimePrice timePrice = (TimePrice) item;
		
		Entry<String, TimePrice> outputEntry =
				new SimpleImmutableEntry<>(MyConstants.KEY_200_POINT, timePrice);
		
		return super.tryEmit(outputEntry);
	}
	
}
