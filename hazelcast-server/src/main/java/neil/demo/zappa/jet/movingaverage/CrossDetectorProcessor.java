package neil.demo.zappa.jet.movingaverage;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;

import neil.demo.zappa.TimePrice;

/**
 * <p>A processor that detects when sequences of numbers on
 * different input streams intersect, and produces output
 * when this occurs.
 * </p>
 * <p>A "<i>cross</i>" will occur when an increasing sequence of
 * points on the first input queue moves above the value of a decreasing
 * sequence of points on the second input queue. The reverse is also the case,
 * if the first input sequence is decreasing and moves below a second input
 * sequence of increasing values.
 * </p>
 * <p>In this demo, the input streams are expected to be a 50-point moving
 * average and a 200-point moving average of the same underlying stream
 * of prices for an item.</p>
 * <p>When the shorter-term moving average crosses above the longer-term
 * moving average, this <i><b>*</b>might<b>*</b></i> be an <i><b>*</b>indication<b>*</b></i>
 * that the price trend is upwards -- known as a "<u>golden cross</u>".
 * </p>
 * <p>The reverse is when the shorter-term moving average crosses below
 * the longer-term moving average, and this <i><b>*</b>might<b>*</b></i> be
 * an <i><b>*</b>indication<b>*</b></i> that the price trend is downwards --
 * known as a "<u>death cross</u>".
 * </p>
 * <p>So what "<u>golden cross</u>" and "<u>death cross</u>" try to do
 * is use analysis of historical prices to predict whether buying or selling
 * is a good idea. This is mathematical sophistry.
 * <p>
 * <ol>
 * <li>Past prices don't predict future prices</li>
 * <li>The 50 point and 200 point averages might not be correct indicators even
 * if the previous point were true. Would 15 and 73 be better for example?
 * </p>
 * </ol>
 * <p>So basically don't use this as a method to invest. If enough people buy
 * because the indicator suggests buy, then the price goes up due to supply
 * and demand, and it might be mistaken that the indicator was correct.
 * </p>
 */
public class CrossDetectorProcessor extends AbstractProcessor {

	private BigDecimal previousOrdinal0Rate = null;
	private BigDecimal currentOrdinal0Rate = null;
	private BigDecimal previousOrdinal1Rate = null;
	private BigDecimal currentOrdinal1Rate = null;

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected boolean tryProcess0(Object item) {
		
		// Re-cast input
		TimestampedEntry<?, Tuple2<Date, List<BigDecimal>>> inputEntry =
			(TimestampedEntry<?, Tuple2<Date, List<BigDecimal>>>) item;

		// Find info 
		Date date = inputEntry.getValue().getKey();
		this.previousOrdinal0Rate = inputEntry.getValue().f1().get(0);
		this.currentOrdinal0Rate = inputEntry.getValue().f1().get(1);
		this.previousOrdinal1Rate = inputEntry.getValue().f1().get(2);
		this.currentOrdinal1Rate = inputEntry.getValue().f1().get(3);

		// Business logic, do they cross ?
		String trend = null;
		if (this.previousOrdinal0Rate.compareTo(this.previousOrdinal1Rate) < 0) {
			if (this.currentOrdinal0Rate.compareTo(this.currentOrdinal1Rate) > 0) {
				trend = "Upward";
			}
		}
		if (this.previousOrdinal0Rate.compareTo(this.previousOrdinal1Rate) > 0) {
			if (this.currentOrdinal0Rate.compareTo(this.currentOrdinal1Rate) < 0) {
				trend = "Downward";
			}
		}
		
		if (trend != null) {
			LocalDate day = TimePrice.convert(date);
			
			Tuple2<LocalDate, String> outputEntryKey
				= Tuple2.tuple2(day, trend);
			
			Tuple2<BigDecimal, BigDecimal> outputEntryValue
				= Tuple2.tuple2(this.currentOrdinal0Rate, this.currentOrdinal1Rate);

			Entry<Tuple2<LocalDate, String>,Tuple2<BigDecimal, BigDecimal>> outputEntry
				= new SimpleImmutableEntry<>(outputEntryKey, outputEntryValue);

			return super.tryEmit(outputEntry);
		}
		return true;
	}

}
