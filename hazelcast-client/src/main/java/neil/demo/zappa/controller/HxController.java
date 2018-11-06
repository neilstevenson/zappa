package neil.demo.zappa.controller;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.Gps;
import neil.demo.zappa.MyConstants;

/**
 * <p>Heathrow Express!
 * </p>
 */
@Controller
@RequestMapping("hx")
@Slf4j
public class HxController {

	private static final DecimalFormat TWO_DP = new DecimalFormat("0.00");
	
	@Autowired
	private HazelcastInstance hazelcastInstance;

	/**
	 * <p>Where is it ? How fast is it going ?
	 * </p>
	 *
	 * @param httpSession
	 * @return
	 */
	@GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("hx/index");

        IMap<String, Gps> positionMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_POSITION);
        List<String> columnsPosition = new ArrayList<>();
        columnsPosition.addAll(Arrays.asList("What","Timestamp","When","Latitude","Longitide","URL"));
        List<Map<String, String>> dataPosition = new ArrayList<>();
        for (Map.Entry<String, Gps> position : positionMap.entrySet()) {
                Map<String, String> datum = new HashMap<>();

                double latitude = position.getValue().getLatitude();
                double longitude = position.getValue().getLongitude();

                datum.put("What", position.getKey());
                datum.put("Timestamp", position.getValue().getTimestamp().toString());
                datum.put("When", new Date(position.getValue().getTimestamp()).toString());
                datum.put("Latitude", String.valueOf(latitude));
                datum.put("LatitudeStr", TWO_DP.format(latitude));
                datum.put("LatitudeStrNS", latitude > 0 ? "N" : "S");
                datum.put("Longitude", String.valueOf(longitude));
                datum.put("LongitudeStr", TWO_DP.format(longitude));
                datum.put("LongitudeStrEW", longitude > 0 ? "E" : "W");
                
                dataPosition.add(datum);
        }

        IMap<String, Double> speedMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_SPEED);
        List<String> columnsSpeed = new ArrayList<>();
        columnsSpeed.addAll(Arrays.asList("What","Timestamp","When","Speed (m/s)"));
        List<List<String>> dataSpeed = new ArrayList<>();
        Set<String> keys = new TreeSet<>(speedMap.keySet());
        for (String key : keys) {
        	Double speed = speedMap.get(key);
        	
        	// Unpick the key
        	String[] tokens = key.split(",");
        	String what = tokens[0];
        	long when = Long.valueOf(tokens[1]);

            List<String> datum = new ArrayList<>();

            datum.add(what);
            datum.add(String.valueOf(when));
            datum.add(new Date(when).toString());
            datum.add(String.valueOf(speed));
    
            dataSpeed.add(datum);
        }
                
        modelAndView.addObject("columnsPosition", columnsPosition);
        modelAndView.addObject("columnsSpeed", columnsSpeed);
        modelAndView.addObject("dataPosition", dataPosition);
        modelAndView.addObject("dataSpeed", dataSpeed); 
        
        return modelAndView;
	}
}
