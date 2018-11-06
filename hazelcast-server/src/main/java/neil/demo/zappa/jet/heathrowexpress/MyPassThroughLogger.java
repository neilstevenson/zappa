package neil.demo.zappa.jet.heathrowexpress;

import com.hazelcast.jet.core.AbstractProcessor;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * An intermediate processor to take some data in from earlier graph vertices,
 * process it, and pass it on to later graph vertices. So this is an
 * intermediate stage rather than a sink which would be more usual for a logger.
 * Just for kicks.
 * </p>
 */
@Slf4j
public class MyPassThroughLogger extends AbstractProcessor {

	private static final String PREFIX = MyPassThroughLogger.class.getSimpleName() + "::";

	/**
	 * <p>
	 * Receive an object, log it, and try to pass it on.
	 * </p>
	 * 
	 * @param ordinal 0 if the default edge from the previous stage
	 * @param item    the output object from the previous stage
	 */
	@Override
	protected boolean tryProcess(int ordinal, Object item) {

		if (ordinal == 0) {
			log.info("{}tryProcess({})", PREFIX, item);
		} else {
			log.info("{}tryProcess(ordinal=={}, {})", PREFIX, ordinal, item);
		}

		return this.tryEmit(item);
	}

}
