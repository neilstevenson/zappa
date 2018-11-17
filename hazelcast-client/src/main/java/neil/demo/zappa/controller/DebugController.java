package neil.demo.zappa.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.hazelcast.core.HazelcastInstance;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.MyConstants;

/**
 * <p>Diagnostic info
 * </p>
 */
@Controller
@RequestMapping("debug")
@Slf4j
public class DebugController {

	@Autowired
	private Environment environment;
	@Autowired
	private HazelcastInstance hazelcastInstance;

	/**
	 * <p>Count the un-expired HTTP sessions
	 * </p>
	 * 
	 * @param httpSession The current one
	 * @return
	 */
	@SuppressWarnings("unchecked")
	@GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("debug/index");

        // Demo only, the size() function runs on all partitions, minor performance cost
        long sessionCount = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_JSESSIONID).size();
        modelAndView.addObject("sessionCount", Long.toString(sessionCount));

        // Connection info heading
        List<String> columns = new ArrayList<>();
        columns.add("Host");
        columns.add("Port");
        columns.add("Member UUID");
        modelAndView.addObject("columns", columns);

        // Connection info detail
        List<List<String>> data = new ArrayList<>();
        this.hazelcastInstance.getCluster().getMembers()
    	.stream()
    	.forEach(member -> {
    		List<String> value = new ArrayList<>();
    		value.add(member.getAddress().getHost());
    		value.add(String.valueOf(member.getAddress().getPort()));
    		value.add(member.getUuid());

    		data.add(value);
    	});
        modelAndView.addObject("data", data);

        // Find host IP from Hazelcast, web server port from Spring
        String port = this.environment.getProperty("local.server.port");
        String socketAddress
    	= this.hazelcastInstance
    	.getLocalEndpoint()
    	.getSocketAddress().toString();
        
        httpSession.setAttribute(MyConstants.ATTRIBUTE_CLIENT_ADDRESS, socketAddress.substring(1));

        socketAddress = socketAddress.substring(1, socketAddress.indexOf(":") + 1) + port;

        // Track in the HTTP session how many times the user visits this page
        Map<String, Integer> hitMap = (Map<String, Integer>) httpSession.getAttribute(MyConstants.ATTRIBUTE_DEBUG_PAGE_VISITS);
        if (hitMap == null) {
        	hitMap = new HashMap<>();
        }
        Integer hits = hitMap.get(socketAddress);
        if (hits == null) {
        	hits = Integer.valueOf(0);
        }
        hits = hits + 1;
        hitMap.put(socketAddress, hits);
        httpSession.setAttribute(MyConstants.ATTRIBUTE_DEBUG_PAGE_VISITS, hitMap);

        return modelAndView;
	}

}
