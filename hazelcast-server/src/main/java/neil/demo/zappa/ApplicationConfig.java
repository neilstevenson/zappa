package neil.demo.zappa;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>"<i>temporary code</i>"
 * </p>
 * <p>This class defines Spring beans to create a single Hazelcast IMDG and Jet
 * server in this JVM.
 * </p>
 * <p>There is a pull request (<a href="https://github.com/spring-projects/spring-boot/issues/8863">Issue 8863</a>)
 * for Spring Boot to do this for us automatically. Once this is merged, the onerous burden of writing the 13
 * lines of code below can be eliminated and this whole class can be deleted.
 * </p>
 */
@Configuration
@Slf4j
public class ApplicationConfig {

	@Bean
	public Config config() {
		Config config = new ClasspathXmlConfig("hazelcast.xml");
		
		boolean k8s = System.getProperty("k8s", "false").equalsIgnoreCase("true");
		log.info("Kubernetes=={}", k8s);
		
		if (k8s) {
			this.adjustForKubernetes(config.getNetworkConfig().getJoin());
		}
		
		return config;
	}
	
	@Bean
    public JetInstance jetInstance(Config config) {
        JetConfig jetConfig = new JetConfig().setHazelcastConfig(config);
        return Jet.newJetInstance(jetConfig);
    }

    @Bean
    public HazelcastInstance hazelcastInstance(CommandListener commandListener, JetInstance jetInstance) {
        HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();

        // React to map changes
        IMap<?, ?> commandMap = hazelcastInstance.getMap(MyConstants.IMAP_NAME_COMMAND);
        commandMap.addLocalEntryListener(commandListener);

        return hazelcastInstance;
    }

    /**
     * XXX
     * 
     * @param joinConfig Part of the main config
     */
    private void adjustForKubernetes(JoinConfig joinConfig) {
    	
    	log.trace("Turn off TcpIpConfig");
    	joinConfig.getTcpIpConfig().setEnabled(false);

    	//TODO Add kubes
	}

}