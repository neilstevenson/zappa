package neil.demo.zappa;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.kubernetes.HazelcastKubernetesDiscoveryStrategyFactory;
import com.hazelcast.kubernetes.KubernetesProperties;
import com.hazelcast.spi.properties.GroupProperty;

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
			this.adjustForKubernetes(config);
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
     * <p>Modify the configuration loaded from the
     * "{@code hazelcast.xml}" for Kubernetes. This shows
     * how configuration can be tweaked as you go from Java.
     * The client does it the other way, having an XML file
     * for each kind ; this is more obvious but has a bit of
     * repetition.
     * </p> 
     * <p>Turn off IP based discovery, where we specify cluster
     * machine IPs, in favour of asking Kubernetes where the
     * machines are.
     * </p>
     * 
     * @param Config Config from "{@code hazelcast.xml}" to amend
     */
    private void adjustForKubernetes(Config config) {
    	
    	JoinConfig joinConfig = config.getNetworkConfig().getJoin();
    	
    	log.trace("Turn off TcpIpConfig");
    	joinConfig.getTcpIpConfig().setEnabled(false);

        // Configure Kubernetes discovery
        HazelcastKubernetesDiscoveryStrategyFactory hazelcastKubernetesDiscoveryStrategyFactory
            = new HazelcastKubernetesDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig =
                new DiscoveryStrategyConfig(hazelcastKubernetesDiscoveryStrategyFactory);
        discoveryStrategyConfig.addProperty(KubernetesProperties.SERVICE_DNS.key(),
                MyConstants.KUBERNETES_HAZELCAST_SERVICE_NAME);
        
        // Activate Kubernetes discovery
        config.setProperty(GroupProperty.DISCOVERY_SPI_ENABLED.toString(), "true");
        joinConfig.getDiscoveryConfig().addDiscoveryStrategyConfig(discoveryStrategyConfig);

        // Where to send to Mancenter, not fatal if it's switched off
        config.getManagementCenterConfig()
        	.setEnabled(true).setUrl("http://" + MyConstants.KUBERNETES_MANCENTER_SERVICE_NAME + ":8080/hazelcast-mancenter");
    }

}