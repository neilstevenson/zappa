package neil.demo.zappa.jet.movingaverage;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.hazelcast.jet.core.AbstractProcessor;

import neil.demo.zappa.CurrencyPairKey;
import neil.demo.zappa.CurrencyPairValue;
import neil.demo.zappa.TimePrice;

/**
 * <p>A moving average calculation that works on a
 * continuous stream of input.
 * </p>
 * <p>The average calculation is the simplest method,
 * numbers are added and divided by the count. There is
 * not any weighting towards later numbers.
 * </p>
 * <p>Calculations use {@link BigDecimal} which is
 * probably an unnecessary level of accuracy for this
 * application.
 * </p>
 * <p>Note in Jet this is run as a continuous stream job.
 * Input will be fed as it arrives to the {@link MovingAverageProcessor#tryProcess tryProcess}
 * method rather than it be sent a complete batch. This
 * method will produce output as soon as it is ready rather
 * than wait for a complete batch.
 * </p>
 */
public class MovingAverageProcessor extends AbstractProcessor {

	private final BigDecimal averageOf;
	private final BigDecimal[] rates;
	private LocalDate day;
	private int count;

	/**
	 * <p>There may be more than one instance of this
	 * class per JVM, depending on how many CPUs are
	 * available.
	 * </p>
	 * <p>Build a storage array which we will use as
	 * a ringbuffer to hold input until we are ready
	 * to output.
	 * </p>
	 *
	 * @param arg0 How many input items to average
	 */
	public MovingAverageProcessor(int arg0) {
		this.averageOf = new BigDecimal(arg0);
		this.rates = new BigDecimal[arg0];
		this.count = 0;
	}

	/**
	 * <p>Use a ringbuffer to produce a moving average from
	 * a constant stream of input.
	 * </p>
	 * <p>The logic for input processing is simply to store
	 * each item in a local ringbuffer.
	 * </p>
	 * <p>The logic for output processing is to determine
	 * if the ringbuffer is fully populated. If it is,
	 * we can calculate and send out the average. If it
	 * isn't, there is no average to send and the method
	 * has nothing further to do.
	 * </p>
	 * <p>Output sending uses Jet's {@link com.hazelcast.jet.core.AbstractProcessor#tryEmit tryEmit}
	 * to see if the {@link com.hazelcast.jet.core.Edge Edge} can take the output
	 * or if it is full of queued items that the next step hasn't handled yet.
	 * </p> 
	 * 
	 * @return True if processing passed on any necessary output
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected boolean tryProcess(int ordinal, Object item) {
		
		/* Extract what we need from input. An optimisation
		 * would be to do this in the projection not this class.
		 */
		ConsumerRecord<CurrencyPairKey, CurrencyPairValue>
			data = (ConsumerRecord<CurrencyPairKey, CurrencyPairValue>) item;

		CurrencyPairValue value = data.value();

		// Track where we are going to store in the ringbuffer
		int position = this.count % this.rates.length;
		this.count++;
		
		// Store input in local ringbuffer
		this.rates[position] = value.getRate();
		this.day = value.getDay();

		// Produce output once we have enough input
		if (count >= this.rates.length) {
			Date date = TimePrice.convert(this.day);
			BigDecimal average = this.calculateAverage();

			// Average up to the day stated, assumes no days missed
			TimePrice timePrice = new TimePrice(date, average);

			// False if needs to back off and rerun
			boolean sent = super.tryEmit(timePrice);
			if (sent==false) {
				count--;
			}
			return sent;
		} else {
			// Nothing to produce
			return true;
		}
	}

	/**
	 * <p>Calculate the <u>simple</u> average of a fully
	 * populated array of numbers. That is, there is no
	 * weighting towards the most recent, all are equal.
	 * So add them up, divide by the count.
	 * </p>
	 * <p>Note this is {@link BigDecimal} so this generates
	 * a lot of intermediate objects. Use of {@code double}
	 * might be sufficient here, we don't need the highest
	 * degree of accuracy.
	 * </p>
	 *
	 * @return Average, to 2 decimal places.
	 */
	private BigDecimal calculateAverage() {
		BigDecimal sum = BigDecimal.ZERO;
		
		for (int i = 0 ; i < this.rates.length ; i ++) {
			sum = sum.add(this.rates[i]);
		}
		
		BigDecimal average = sum.divide(this.averageOf, 2, RoundingMode.HALF_UP);
		
		return average;
	}

	
}
