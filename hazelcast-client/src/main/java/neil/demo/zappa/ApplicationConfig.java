package neil.demo.zappa;

import java.io.IOException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

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
public class ApplicationConfig {
	
	@Bean
	public ClientConfig clientConfig() throws IOException {
		return new XmlClientConfigBuilder("hazelcast-client.xml").build();
	}
	
	@Bean
	public JetInstance jetInstance(ClientConfig clientConfig) throws Exception {
		return Jet.newJetClient(clientConfig);
    }

	@Bean
	public HazelcastInstance hazelcastInstance(JetInstance jetInstance) {
		return jetInstance.getHazelcastInstance();
	}

}