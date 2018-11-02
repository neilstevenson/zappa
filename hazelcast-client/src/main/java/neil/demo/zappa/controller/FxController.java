package neil.demo.zappa.controller;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.datamodel.Tuple2;

import lombok.extern.slf4j.Slf4j;
import neil.demo.zappa.MyConstants;
import neil.demo.zappa.TimePrice;

/**
 * <p>Foreign Exchange
 * </p>
 * @param <BigDecimal>
 */
@Controller
@RequestMapping("fx")
@Slf4j
public class FxController {

	@Autowired
	private HazelcastInstance hazelcastInstance;

	/**
	 * <p>Is there anything in the alerts map?
	 * </p>
	 * 
	 * @param httpSession
	 * @return
	 */
	@GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("fx/index");

        List<String> columns = new ArrayList<>();
        columns.add("Date");
        columns.add("Cross");
        columns.add("50-Point");
        columns.add("200-Point");
        modelAndView.addObject("columns", columns);
        
        List<List<String>> data = new ArrayList<>();
        modelAndView.addObject("data", data);

        IMap<Tuple2<LocalDate, String>, Tuple2<BigDecimal, BigDecimal>> 
    		alertsMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_ALERT);

        for (Map.Entry<Tuple2<LocalDate, String>, Tuple2<BigDecimal, BigDecimal>> entry : alertsMap.entrySet()) {
        	List<String> datum = new ArrayList<>();
        	
        	datum.add(entry.getKey().f0().toString());
        	datum.add(entry.getKey().f1());
        	datum.add(entry.getValue().f0().toString());
        	datum.add(entry.getValue().f1().toString());
        	
        	data.add(datum);
        }
 
        return modelAndView;
	}
	
	/**
	 * <p>Is there anything in the Bitcoin prices map?
	 * </p>
	 * 
	 * @param httpSession
	 * @return
	 */
	@GetMapping("/index2")
    public ModelAndView index2(HttpSession httpSession) {
        log.info("index2(), session={}", httpSession.getId());

        ModelAndView modelAndView = new ModelAndView("fx/index2");
        
        List<String> columns = new ArrayList<>();
        modelAndView.addObject("columns", columns);
        columns.add("Style");
        columns.add("Rate");
        
        List<List<String>> data = new ArrayList<>();
        modelAndView.addObject("data", data);

        IMap<String, TimePrice> btcUsdMap = this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_BTC_USD);

        for (Map.Entry<String, TimePrice> entry : btcUsdMap.entrySet()) {
        	List<String> datum = new ArrayList<>();
        	
        	datum.add(entry.getKey());
        	datum.add(entry.getValue().getRate().toString());
        	
        	data.add(datum);
        }
        
        return modelAndView;
	}
}
