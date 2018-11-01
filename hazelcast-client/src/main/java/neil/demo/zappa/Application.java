package neil.demo.zappa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * <p>Start <a href="https://spring.io/projects/spring-boot">Spring Boot</a>
 * and let it do the rest.
 * </p>
 */
@SpringBootApplication
public class Application {

	/* These two properties are for the graph viewer
	 */
    static {
        // No controlling parent window
        System.setProperty("java.awt.headless", "false");
        // Reduce contention, data updates all sent to same panel
        System.setProperty("hazelcast.client.event.thread.count", "1");
    }

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}
