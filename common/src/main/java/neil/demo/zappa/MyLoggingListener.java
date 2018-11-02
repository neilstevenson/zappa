package neil.demo.zappa;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Log data inserts and changes. A new value is relevant for
 * these so log it out. For deletes, the new value would be null.
 * </p>
 */
@SuppressWarnings("rawtypes")
@Slf4j
public class MyLoggingListener implements EntryAddedListener, EntryUpdatedListener {
	
	@Override
	public void entryUpdated(EntryEvent event) {
		this.logIt(event);
	}

	@Override
	public void entryAdded(EntryEvent event) {
		this.logIt(event);
	}

	private void logIt(EntryEvent event) {
		log.info("Map '{}' {} : {} {}",
				event.getSource(),
				event.getEventType(),
				event.getKey(),
				event.getValue()
				);
	}

}
