package neil.demo.zappa;

import java.time.LocalDateTime;

import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * <p>Topic logging listener.
 * </p>
 * <p>If something is published to a topic, and this class is
 * registered as a listener, it'll do a {@code toString()}
 * to print the message payload.
 * </p>
 */
@SuppressWarnings("rawtypes")
public class MyTopicListener implements MessageListener {

	/**
	 * <p>Dump a received message to the logger.
	 * </p>
	 *
	 * @param message From the topic
	 */
	@Override
	public void onMessage(Message message) {
		String payload = message.getMessageObject().toString();
		System.out.println("");
		System.out.println("**************************************************"
				+ " ALERT "
				+ "**************************************************");
		System.out.format("%s: Topic '%s': %s%n",
				LocalDateTime.now(), message.getSource(), payload);
		System.out.println("**************************************************"
				+ " ALERT "
				+ "**************************************************");
		System.out.println("");
	}

}
