package neil.demo.zappa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;

import neil.demo.zappa.panel.PricePanelListener;

/**
 * <p>Listen on all message topics for activity, and
 * on Bitcoin to draw a graph.
 * </p>
 */
@Configuration
public class ApplicationInitialiser implements CommandLineRunner {
	
	@Autowired
	private HazelcastInstance hazelcastInstance;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void run(String... args) throws Exception {
		// Listed on all topics
		for (String iTopicName : MyConstants.ITOPIC_NAMES) {
            ITopic iTopic = this.hazelcastInstance.getTopic(iTopicName);
            iTopic.addMessageListener(new MyTopicListener());
		}
		
		// Listen on the Bitcoin map
        this.hazelcastInstance
        .getMap(MyConstants.IMAP_NAME_BTC_USD)
        .addEntryListener(new PricePanelListener(), true);
	}

}