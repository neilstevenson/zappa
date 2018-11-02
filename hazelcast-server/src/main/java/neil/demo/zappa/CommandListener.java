package neil.demo.zappa;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.jet.account.Account;
import neil.demo.zappa.jet.movingaverage.MovingAverageDAG;
import neil.demo.zappa.jet.wordcount.WordCount;

/**
 * <p>An implementation of the command pattern for control of Jet
 * jobs. Put "{@code (something,[start])}" into the command map to
 * indicate the "<i>something</i>" job is to be started. Put "{@code (something,[stop])}" 
 * to indicate the "</i>something</i>" job is to be stopped. The basic structure
 * being the map takes <i>(noun, verb)</i> pairs and the map listener
 * does the job control.
 * </p>
 * <p>This approach has two main benefits over directly submitting the Jet job:
 * <ol>
 * <li>The commands are Strings, so supported by all client languages.
 * Any client can request a Jet job.
 * </li>
 * <li>The listener can ignore the request if it likes, so the job isn't
 * started twice if two places request it.
 * </li>
 * </ol>
 * <p>The format is <i>(noun, verb)</i> as {@code key-value} rather than
 * <i>(verb, noun)</i>. This is so all commands for a given noun are
 * handled by the same JVM, although this isn't strictly necessary.
 * </p>
 * <p>Also, the value part is actually an array of strings, with the
 * first being the verb. There can be others, as a way to pass params
 * to the job.
 * </p>
 * <p><b>Note : Race condition</b> the methods {@link #handleStart} and
 * {@link #handleStop} check with Jet that the named job isn't already
 * in that state (already started or already stopped respectively)
 * before actioning. However, this class doesn't use any locking so there's
 * a brief window where another request arriving could trigger the
 * same action (attempt to start twice or attempt to stop twice).
 * Jet allows a job to be run multiple times but we don't want
 * that here so should really handle this.
 * </p>
 */
@Component
@Slf4j
public class CommandListener implements EntryAddedListener<String, List<String>>, EntryUpdatedListener<String, List<String>> {

	@Autowired
    private JetInstance jetInstance;

    /**
     * <p>Insert and update events are handled the same.
     * </p>
     * @param arg0 Map entry event, UPDATED
     */
    @Override
    public void entryUpdated(EntryEvent<String, List<String>> arg0) {
        try {
            this.handle(arg0);
        } catch (Exception e) {
            log.error("entryUpdated", e);
        }
    }

    /**
     * <p>Insert and update events are handled the same.
     * </p>
     * @param arg0 Map entry event, ADDED
     */
    @Override
    public void entryAdded(EntryEvent<String, List<String>> arg0) {
        try {
            this.handle(arg0);
        } catch (Exception e) {
            log.error("entryAdded", e);
        }
    }

    /**
     * <p>Pick apart the noun and verb from the map event, and perform
     * the corresponding verb action.
     * </p>
     * 
     * @param arg0 Map entry event, ADDED or UPDATED
     * @throws Exception From Jet
     */
    private void handle(EntryEvent<String, List<String>> arg0) throws Exception {
        log.trace("'{}' '{}'", arg0.getKey(), arg0.getValue());
        
        String noun = arg0.getKey();
        List<String> params = arg0.getValue();
        String verb = params.get(0);

        if (verb.equalsIgnoreCase(MyConstants.COMMAND_START)) {
            this.handleStart(noun, (params.size() == 1 ? null : params.get(1)));
        } else {
            if (verb.equalsIgnoreCase(MyConstants.COMMAND_STOP)) {
                this.handleStop(noun);
            } else {
                log.error("Unknown command verb '{}'", verb);
            }
        }
    }

    /**
     * <p>Create a Jet job based on the provided job name.
     * </p>
     * <p>See class-level comment about race condition.
     * </p>
     *
     * @param noun Name of the job to create
     * @param params Any params for the constructor
     */
    private void handleStart(String noun, String params) {
    	Job job = this.jetInstance.getJob(noun);
    	if (job!=null) {
            log.info("Ignoring start request, '{}' job id {} has status '{}'",
            		noun, job.getId(), job.getStatus());
            return;
    	}

    	DAG dag = null;
    	JobConfig jobConfig = new JobConfig();
    	jobConfig.setName(noun);
    	
    	if (noun.equals(MyConstants.JOB_NAME_MOVING_AVERAGE)) {
    		if (params!=null) {
        		dag = MovingAverageDAG.build(params);
    		} else {
    			log.error("Params null for {}", noun); 
    		}
    	}
    	if (noun.equals(MyConstants.JOB_NAME_ACCOUNT)) {
    		if (params!=null) {
    			Pipeline pipeline = Account.build(params);
        		dag = pipeline.toDag();
    		} else {
    			log.error("Params null for {}", noun); 
    		}
    	}
    	if (noun.equals(MyConstants.JOB_NAME_WORD_COUNT)) {
    		Pipeline pipeline = WordCount.build();
    		dag = pipeline.toDag();
    	}
    	
    	if (dag == null) {
    		log.error("Unknown command noun '{}'", noun);
    		return;
    	}
    	
    	job = this.jetInstance.newJob(dag, jobConfig);
        log.info("Started '{}', job id {}", noun, job.getId());
    }

    /**
     * <p>Find a job by name, and if found cancel it.
     * </p>
     * <p>See class-level comment about race condition.
     * </p>
     * 
     * @param noun The name of a Jet job
     * @throws Exception If cancel fails, eg. already in progress
     */
    private void handleStop(String noun) throws Exception {
    	Job job = this.jetInstance.getJob(noun);
    	
        if (job==null) {
            log.info("Ignoring stop request, '{}' job never started", noun);
            return;
        }
        
        if (job.getStatus()!=JobStatus.RUNNING) {
        	log.info("Ignoring stop '{}' request, job id {} has status {}",
        			noun, job.getId(), job.getStatus());
        } else {
        	try {
            	job.cancel();
        		log.info("Stop request for '{}' : requested : job {}", 
        				noun, job.getId());
        	} catch (Exception e) {
        		String tag = String.format("handleStop(%s)", noun);
        		log.error(tag, e);
            }
     	}
    }
}
