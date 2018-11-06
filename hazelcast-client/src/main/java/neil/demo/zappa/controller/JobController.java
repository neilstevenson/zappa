package neil.demo.zappa.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.hazelcast.core.ISet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.MyConstants;

@Controller
@RequestMapping("jobs")
@Slf4j
public class JobController {

	@Autowired
	private JetInstance jetInstance;
	@Value("${bootstrap-servers}")
	private String bootstrapServers;

	/**
	 * <p>Find out information about the named jobs. This may not
	 * be all of them, there may be more names than we know, names
	 * may be duplicated, names may be omitted, etc.
	 * </p>
	 * 
	 * @param httpSession
	 * @return
	 */
    @GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("jobs/index");

        ISet<String> jetJobSet = this.jetInstance.getHazelcastInstance().getSet(MyConstants.ISET_NAME_JET_JOBS);
        
        List<String> columns = new ArrayList<>();
        columns.add("Job");
        columns.add("Id");
        columns.add("Submission Time");
        columns.add("Start Params");
        columns.add("Status");
        columns.add("Action");

        Map<String, Map<String, String>> data = new TreeMap<>();
        jetJobSet.stream().forEach(jobName -> {
        	Job job = this.jetInstance.getJob(jobName);

        	Map<String, String> datum = new HashMap<>();

    		if (jobName.equals(MyConstants.JOB_NAME_WORD_COUNT)
    				|| jobName.equals(MyConstants.JOB_NAME_HEATHROW_EXPRESS_2)) {
           		datum.put("Start Params", "");
    		} else {
           		datum.put("Start Params", this.bootstrapServers);
    		}
        	
        	// Start or existing ?
        	if (job==null) {
        		if (datum.get("Start Params").equals("Missing")) {
        			datum.put("Action", "Missing Params");
        		} else {
        			datum.put("Action", "Start");
        		}
        	} else {
        		datum.put("Id", String.valueOf(job.getId()));
        		datum.put("Submission Time", new Date(job.getSubmissionTime()).toString());
        		
        		datum.put("Status", String.valueOf(job.getStatus()));
        		if (job.getStatus()==JobStatus.RUNNING) {
            		datum.put("Action", "Stop");
        		} else {
            		datum.put("Action", "N/a");
        		}
        	}
        	
        	data.put(jobName, datum);
        });      
        
        modelAndView.addObject("columns", columns);
        modelAndView.addObject("data", data);
                
        return modelAndView;
    }

    @GetMapping("/submit")
    public ModelAndView submit(HttpServletRequest httpServletRequest, HttpSession httpSession) {
    	String j_noun = httpServletRequest.getParameter("j_noun");
    	if (j_noun==null) {
    		j_noun="";
    	}
    	String j_verb = httpServletRequest.getParameter("j_verb");
    	if (j_verb==null) {
    		j_verb="";
    	}
        log.info("submit({}, {}), session={}", j_noun, j_verb, httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("jobs/index2");

        // Check noun provided
        ISet<String> jetJobSet = this.jetInstance.getHazelcastInstance().getSet(MyConstants.ISET_NAME_JET_JOBS);
        boolean found = false;
        for (String name : jetJobSet) {
        	if (name.equalsIgnoreCase(j_noun)) {
        		j_noun = name;
        		found = true;
        	}
        }
        if (!found) {
        	return modelAndView;
        }
        
        // Check verb provided
        if (MyConstants.COMMAND_START.equalsIgnoreCase(j_verb)) {
        	j_verb = MyConstants.COMMAND_START;
        } else {
            if (MyConstants.COMMAND_STOP.equalsIgnoreCase(j_verb)) {
            	j_verb = MyConstants.COMMAND_STOP;
            } else {
            	return modelAndView;
            }
        }
        
        // Build params
        List<String> j_params = new ArrayList<>();
        j_params.add(j_verb);

        // Augment params
        if (MyConstants.COMMAND_START.equals(j_verb)) {
        	if (MyConstants.JOB_NAME_ACCOUNT.equals(j_noun)
        			|| MyConstants.JOB_NAME_HEATHROW_EXPRESS_1.equals(j_noun)
        			|| MyConstants.JOB_NAME_MOVING_AVERAGE.equals(j_noun) ) {
            	j_params.add(this.bootstrapServers);
        	}
        }

        // Request
        this.jetInstance.getHazelcastInstance()
        	.getMap(MyConstants.IMAP_NAME_COMMAND)
        	.put(j_noun, j_params);

        modelAndView.addObject("j_noun", j_noun);
        modelAndView.addObject("j_params", j_params);
        
        return modelAndView;
    }
}
