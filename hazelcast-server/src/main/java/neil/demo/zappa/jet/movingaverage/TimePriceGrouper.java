package neil.demo.zappa.jet.movingaverage;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.hazelcast.jet.datamodel.Tuple2;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.TimePrice;

/**
 * <p>Group two windows of moving average data, arriving on
 * ordinals 0 and 1, into a single output.
 * </p>
 * <p>We get at most 2 input records on each ordinal.
 * </p>
 */
@Data
@Slf4j
@SuppressWarnings("serial")
public class TimePriceGrouper implements Serializable {

	private List<BigDecimal> ordinal0_rate = new ArrayList<>();
	private List<BigDecimal> ordinal1_rate = new ArrayList<>();
	private List<Date> ordinal0_date = new ArrayList<>();
	private List<Date> ordinal1_date = new ArrayList<>();

	/**
	 * <p>Add a price received on channel 0 to the current
	 * object. Since window size is 2, don't expect
	 * a third value on the channel.
	 * </p>
	 * 
	 * @param timePrice
	 * @return
	 */
	public TimePriceGrouper add0(TimePrice timePrice) {
		if (this.ordinal0_rate.size() < 2) {
			this.ordinal0_rate.add(timePrice.getRate());
			this.ordinal0_date.add(timePrice.getDate());
		} else {
			log.error("add0 Received {} but full", timePrice);
		}
		return this;
	}
	
	/**
	 * <p>Add a price received on channel 1 to the current
	 * object. Since window size is 2, don't expect
	 * a third value on the channel.
	 * </p>
	 * 
	 * @param timePrice
	 * @return
	 */
	public TimePriceGrouper add1(TimePrice timePrice) {
		if (this.ordinal1_rate.size() < 2) {
			this.ordinal1_rate.add(timePrice.getRate());
			this.ordinal1_date.add(timePrice.getDate());
		} else {
			log.error("add1 Received {} but full", timePrice);
		}
		return this;
	}

	/**
	 * <p>Merge processor, combines two co-groups into
	 * one, remote into current.
	 * </p>
	 *
	 * @param that
	 * @return
	 */
	public TimePriceGrouper combine(TimePriceGrouper that) {

		for (int i=0 ; i < that.getOrdinal0_date().size() ; i++) {
			Date date = that.getOrdinal0_date().get(i);
			BigDecimal rate = that.getOrdinal0_rate().get(i);
			
			// If date same so must rate be, as same ordinal
			if (!this.ordinal0_date.contains(date)) {
				this.ordinal0_date.add(date);
				this.ordinal0_rate.add(rate);
			}
		}
		for (int i=0 ; i < that.getOrdinal1_date().size() ; i++) {
			Date date = that.getOrdinal1_date().get(i);
			BigDecimal rate = that.getOrdinal1_rate().get(i);
			
			// If date same so must rate be, as same ordinal
			if (!this.ordinal1_date.contains(date)) {
				this.ordinal1_date.add(date);
				this.ordinal1_rate.add(rate);
			}
		}
		
		return this;
	}
	
	/**
	 * <p>If the data captured is complete, return it
	 * </p>
	 * 
	 * @return Null if didn't get two values from each input
	 */
	public Tuple2<Date, List<BigDecimal>> get() {
		// Should not end up with more than 2
		if (this.ordinal0_date.size() > 2
				|| this.ordinal1_date.size() > 2) {
			log.trace("get() has {} and {}", this.ordinal0_date.size(), this.ordinal1_date.size());
			return null;
		}
		
		// Incomplete or exact
		if (this.ordinal0_date.size() !=2
			|| this.ordinal1_date.size() != 2
			|| !this.ordinal0_date.get(1).equals(this.ordinal1_date.get(1))) {
			return null;
		} else {
			List<BigDecimal> rates = new ArrayList<>(4);
			rates.addAll(this.ordinal0_rate);
			rates.addAll(this.ordinal1_rate);
			// Same date on ordinal 0 as ordinal 1, 2nd in list is newer
			return Tuple2.tuple2(this.ordinal0_date.get(1), rates);
		}
	}
}
