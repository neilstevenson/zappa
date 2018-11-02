package neil.demo.zappa.jet.movingaverage;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map.Entry;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.Tuple2;

import neil.demo.zappa.MyConstants;

/**
 * <p>Produces an alert text message to a Hazelcast topic, as the
 * reformatting of the input. Essentially the input item is the
 * alert, all we do with this vertex is encapsulate the formatting
 * so this could be changed if need be. Similarly for where the
 * alert is sent. Here we send it to a Hazelcast topic, for any
 * subscribers to see.
 * </p>
 * <p>As an optimisation, implement {@link #AlertToTopicPublisher#tryProcess0}
 * rather than the more generic {@link #tryProcess(int, Object)}. We
 * only wire this vertex to a single input, so can use 
 * {@link #tryProcess0(Object)} to process the input queue for the
 * first ordinal (ordinal 0) rather than the generic version
 * {@link #tryProcess(int, Object)} which gets the ordinal number
 * as a parameter. It doesn't make a lot of difference here, just
 * a demo of different approaches.
 * </p>
 */
public class AlertToTopicProcessor extends AbstractProcessor {

	private HazelcastInstance hazelcastInstance;
	
	@Override
	protected void init(Context context) throws Exception {
		super.init(context);
		this.hazelcastInstance = context.jetInstance().getHazelcastInstance();
	}

	/**
	 * <p>Process an incoming item by formatting and sending it to a topic.
	 * </p>
	 * <p>The input is a map entry, in case we decide later to save it
	 * to a Hazelcast {@link com.hazelcast.core.IMap IMap}.
	 * </p>
	 * <p>The entry key has the date of the price cross and the direction,
	 * golden cross (good) or death cross (good). The entry value has the
	 * values for the 50-point and 200-point averages at the cross.
	 * </p>
	 * <p>Both upwards and downwards trends are good if you're a trader,
	 * they make money when the price changes. Perhaps not so good if you
	 * are an investor to see the death cross.
	 * </p>
	 * 
	 * @param item Raw data to format and publish
	 * @return True, this is a sink, output queue can't be full as not used
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected boolean tryProcess0(Object item) {
		ITopic<String> alertTopic = this.hazelcastInstance.getTopic(MyConstants.ITOPIC_NAME_ALERT);
		
		// Re-cast input
		Entry<Tuple2<LocalDate, String>,Tuple2<BigDecimal, BigDecimal>> entry =
				(Entry<Tuple2<LocalDate, String>, Tuple2<BigDecimal, BigDecimal>>) item;

		// Fields from input
		LocalDate day = entry.getKey().getKey();
		String trend = entry.getKey().getValue();
		BigDecimal current50point = entry.getValue().getKey();
		BigDecimal current200point = entry.getValue().getValue();

		// Alert text
		String cross = (trend.equalsIgnoreCase("upward") ? "Golden Cross" : "Death Cross");
		String alert = cross + " at " + day 
					+ " (50-point $" + current50point + ", 200-point $" + current200point + ")";

		// Send and we're done
		alertTopic.publish(alert);
		return true;
	}

}
