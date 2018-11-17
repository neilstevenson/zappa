package neil.demo.zappa;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>"<i>temporary code</i>"
 * </p>
 * <p>This class defines Spring beans to create a single Hazelcast IMDG and Jet
 * client in this JVM.
 * </p>
 * <p>As for the server-side, until (<a href="https://github.com/spring-projects/spring-boot/issues/8863">Issue 8863</a>)
 * is merged we have to do this manually. Such a chore.
 * </p>
 */
@Configuration
@Slf4j
public class ApplicationConfig {
	
	@Bean
	public ClientConfig clientConfig() throws IOException {
		ClientConfig clientConfig = new XmlClientConfigBuilder("hazelcast-client.xml").build();
		
		boolean k8s = System.getProperty("k8s", "false").equalsIgnoreCase("true");
		log.info("Kubernetes=={}", k8s);

		return clientConfig;
	}
	
	@Bean
	public JetInstance jetInstance(ClientConfig clientConfig) throws Exception {
		return Jet.newJetClient(clientConfig);
    }

	@Bean
	public HazelcastInstance hazelcastInstance(JetInstance jetInstance) {
		HazelcastInstance hazelcastInstance = jetInstance.getHazelcastInstance();
		
        // React to map changes
        IMap<?, ?> accountMap = hazelcastInstance.getMap(MyConstants.IMAP_NAME_ACCOUNT);
        accountMap.addEntryListener(new MyLoggingListener(), true);

		return hazelcastInstance;
	}

}