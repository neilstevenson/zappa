package neil.demo.zappa.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.MyConstants;

/**
 * <p>Collect the lines from Hamlet's speech, and make them
 * available to the HTML page.
 * </p>
 */
@Controller
@RequestMapping("hamlet")
@Slf4j
public class HamletController {

	@Autowired
	private HazelcastInstance hazelcastInstance;
	
    @GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());
        
        IMap<Integer, String> hamletMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_HAMLET);

        ModelAndView modelAndView = new ModelAndView("hamlet/index");

        List<String> data = new ArrayList<>();
        Set<Integer> keys = 
        		hamletMap.keySet()
        		.stream()
        		.collect(Collectors.toCollection(TreeSet::new));
        keys.stream().forEach(key -> data.add(hamletMap.get(key)));
        
        modelAndView.addObject("data", data);
                
        return modelAndView;
    }
	
    @GetMapping("/index2")
    public ModelAndView index2(HttpSession httpSession) {
        log.info("index2(), session={}", httpSession.getId());
        
        IMap<String, Integer> wordMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_WORDS);

        ModelAndView modelAndView = new ModelAndView("hamlet/index2");

        Map<String, String> data = new TreeMap<>();
        Set<String> keys = 
        		wordMap.keySet()
        		.stream()
        		.collect(Collectors.toCollection(TreeSet::new));
        keys.stream().forEach(key -> data.put(key, String.valueOf(wordMap.get(key))));
        
        modelAndView.addObject("data", data);
                
        return modelAndView;
    }
}
