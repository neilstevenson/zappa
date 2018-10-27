package neil.demo.zappa;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Hazelcast objects are usually created on demand. Create
 * them at start-up instead, so we can see their content in the
 * Management Center.
 * </p>
 */
@Configuration
@Slf4j
public class ApplicationInitialiser implements CommandLineRunner {
	
	@Autowired
	private HazelcastInstance hazelcastInstance;

	@Override
	public void run(String... args) throws Exception {
        for (String mapName : MyConstants.IMAP_NAMES) {
            this.hazelcastInstance.getMap(mapName);
        }
        for (String setName : MyConstants.ISET_NAMES) {
            this.hazelcastInstance.getSet(setName);
        }
        for (String topicName : MyConstants.ITOPIC_NAMES) {
            this.hazelcastInstance.getTopic(topicName);
        }
        
        // Test data load, if not already loaded
		this.loadHamlet();
		this.loadJetJobs();
	}
	
	/**
	 * <p>Load Hamlet's speech, one line at a time.
	 * </p>
	 */
    public void loadHamlet() {
        IMap<Integer, String> hamletMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_HAMLET);

        if (!hamletMap.isEmpty()) {
        	log.info("Skip loading '{}', not empty", hamletMap.getName());
        } else {
        	for (int i=0 ; i< TestData.HAMLET.length; i++) {
        		int key = i+1;
        		String value = TestData.HAMLET[i];
        		hamletMap.put(key, value);
        	}
        	log.info("Loaded {} into '{}'", TestData.HAMLET.length, hamletMap.getName());
        }
    }

    /**
     * <p>Load the allowed Jet jobs. Would be nice to use reflection
     * to find these :-)
     * </p>
     */
    public void loadJetJobs() {
    	ISet<String> jetJobSet = this.hazelcastInstance.getSet(MyConstants.ISET_NAME_JET_JOBS);

    	if (!jetJobSet.isEmpty()) {
            log.info("Skip loading '{}', not empty", jetJobSet.getName());
    	} else {
            for (int i=0 ; i< MyConstants.JOB_NAMES.length; i++) {
                    String tag = MyConstants.JOB_NAMES[i];
                    jetJobSet.add(tag);
            }
            log.info("Loaded {} into '{}'", MyConstants.JOB_NAMES.length, jetJobSet.getName());
    	}
    }
}