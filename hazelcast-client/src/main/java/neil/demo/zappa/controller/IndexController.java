package neil.demo.zappa.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import com.hazelcast.core.HazelcastInstance;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.MyConstants;

/**
 * <p>A controller for the front page, "{@code index.html}" that
 * drives the main menu.
 * </p>
 */
@Controller
@Slf4j
public class IndexController {

	@Autowired
	private Environment environment;
	@Autowired
	private HazelcastInstance hazelcastInstance;

    @SuppressWarnings("unchecked")
	@GetMapping("/")
    public String index(HttpSession httpSession, HttpServletRequest httpServletRequest) {
        log.info("index(), session={}", httpSession.getId());

        // Capture the user agent (ie. Chrome, Safari, Firefox)
        if (httpSession.getAttribute(HttpHeaders.USER_AGENT) == null) {
            String userAgent = httpServletRequest.getHeader(HttpHeaders.USER_AGENT);
            httpSession.setAttribute(HttpHeaders.USER_AGENT, (userAgent == null ? "null" : userAgent));
        }

        // Find host IP from Hazelcast, web server port from Spring
        String port = this.environment.getProperty("local.server.port");
        String socketAddress
    	= this.hazelcastInstance
    	.getLocalEndpoint()
    	.getSocketAddress().toString();
        socketAddress = socketAddress.substring(1, socketAddress.indexOf(":") + 1) + port;

        // Track in the HTTP session how many times the user visits this page
        Map<String, Integer> hitMap = (Map<String, Integer>) httpSession.getAttribute(MyConstants.ATTRIBUTE_MAIN_MENU_VISITS);
        if (hitMap == null) {
        	hitMap = new HashMap<>();
        }
        Integer hits = hitMap.get(socketAddress);
        if (hits == null) {
        	hits = Integer.valueOf(0);
        }
        hits = hits + 1;
        hitMap.put(socketAddress, hits);
        httpSession.setAttribute(MyConstants.ATTRIBUTE_MAIN_MENU_VISITS, hitMap);
        
        return "index";
    }

}
