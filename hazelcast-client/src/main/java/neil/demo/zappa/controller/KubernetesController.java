package neil.demo.zappa.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hazelcast.core.HazelcastInstance;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Provide a HTTP endpoint to test if this Hazelcast client is happy.
 * Kubernetes will use this to test that the JVM is up.
 * </p>
 * <p>
 * The Hazelcast server provides a similar but not identical
 * implementation. Kubernetes only cares for HTTP 200.
 * </p>
 * <p>
 * You can test this with:
 * <pre>
 * curl -v http://localhost:8082/k8s
 * </pre>
 * substituting the port you've selected if not 8082.
 * </p>
 */
@RestController
@Slf4j
public class KubernetesController {

    @Autowired
    private HazelcastInstance hazelcastInstance;

    /**
     * @return {@code true} if good, exception otherwise
     */
    @GetMapping("/k8s")
    public String k8s() {
        log.info("k8s()");

        Boolean running = this.hazelcastInstance.getLifecycleService().isRunning();

        if (running) {
            return running.toString();
        } else {
            throw new RuntimeException("Running==" + running);
        }
    }

}
