package neil.demo.zappa;

import java.util.Currency;
import java.util.Locale;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.web.WebFilter;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>Additional configuration so that this JVM is both a
 * Hazelcast client and web-server, and stores the HTTP
 * sessions of the web-server via the Hazelcast client
 * into the remote Hazelcast server.
 * </p>
 */
@Configuration
@Slf4j
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
	 * <p>For displaying on the account pages. Country code
	 * may be empty string on containers.
	 * </p>
	 */
    @Bean
    public String currencySymbol() {
    	try {
        	Locale locale = Locale.getDefault();
    		if (locale.getCountry()!=null && locale.getCountry().length() > 0) {
    			return Currency.getInstance(locale).getSymbol();
    		}
    	} catch (Exception e) {
    		log.error("currencySymbol", e);
    	}
		return Currency.getInstance("GBP").getSymbol();
    }
}