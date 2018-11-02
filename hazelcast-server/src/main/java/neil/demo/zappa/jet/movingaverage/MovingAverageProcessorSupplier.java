package neil.demo.zappa.jet.movingaverage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.hazelcast.jet.core.ProcessorSupplier;

/**
 * <p>A factory to provide customised instances of the
 * {@link MovingAverageProcessor} class.
 * <p>
 * <p>Customisation is simply to provide the constructor
 * argument, for the number of items to form the average
 * from. Average of 10, average of 230, etc.
 * </p>
 */
@SuppressWarnings("serial")
public class MovingAverageProcessorSupplier implements ProcessorSupplier {

	private final int averageOf;

	public MovingAverageProcessorSupplier(int arg0) {
		this.averageOf = arg0;
	}

	/**
	 * <p>This method is called on each JVM to build a number of
	 * {@link MovingAverageProcessor} instances to run on this
	 * JVM. One per CPU on a multiple CPU machine results in
	 * multiple instances on the JVM, so all CPUs are used
	 * for processing.
	 * </p>
	 *
	 * @param count Number of instances to create
	 * @return A collection of those instances
	 */
	@Override
	public Collection<MovingAverageProcessor> get(int count) {
		List<MovingAverageProcessor> result = new ArrayList<>();

		for (int i = 0 ; i < count ; i++) {
			result.add(new MovingAverageProcessor(this.averageOf));
		}
		
		return result;
	}
	
}