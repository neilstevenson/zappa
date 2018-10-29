package neil.demo.zappa;

import java.util.Currency;
import java.util.Locale;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.web.WebFilter;

/**
 * <p>Additional configuration so that this JVM is both a
 * Hazelcast client and web-server, and stores the HTTP
 * sessions of the web-server via the Hazelcast client
 * into the remote Hazelcast server.
 * </p>
 */
@Configuration
public class ApplicationWebConfig {

	@Autowired
	private HazelcastInstance hazelcastInstance;

	@Bean
    public WebFilter webFilter() {
        Properties properties = new Properties();

        properties.put("map-name", MyConstants.IMAP_NAME_JSESSIONID);
        properties.put("instance-name", this.hazelcastInstance.getName());
        properties.put("sticky-session", "false");
        properties.put("use-client", "true");
                    
        return new WebFilter(properties);
	}
	
	/**
	 * <p>For displaying on the account pages
	 * </p>
	 */
    @Bean
    public String currencySymbol() {
        return Currency.getInstance(Locale.getDefault()).getSymbol();
    }
}