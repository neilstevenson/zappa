package neil.demo.zappa.controller;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
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
import neil.demo.zappa.AccountBaseline;
import neil.demo.zappa.AccountTransaction;
import neil.demo.zappa.MyConstants;

/**
 * <p>Display the "{@code Account}" map.
 * </p>
 */
@Controller
@RequestMapping("account")
@Slf4j
public class AccountController {

	private static final DecimalFormat TWO_DP = new DecimalFormat("0.00");

	@Autowired
	private HazelcastInstance hazelcastInstance;
	@Autowired
	private String currencySymbol;

	/**
	 * <p>Basic account info, balances
	 * </p>
	 *
	 * @param httpSession
	 * @return
	 */
    @GetMapping("/index")
    public ModelAndView index(HttpSession httpSession) {
        log.info("index(), session={}", httpSession.getId());
        
        IMap<String, Tuple2<AccountBaseline, List<AccountTransaction>>> accountMap = 
        		this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_ACCOUNT);

        ModelAndView modelAndView = new ModelAndView("account/index");

        List<String> columns = new ArrayList<>();
        columns.add("Account Id");
        columns.add("Account Holder");
        columns.add("Last Update");
        columns.add("Balance");
        modelAndView.addObject("columns", columns);
        
        List<List<String>> data = new ArrayList<>();
        modelAndView.addObject("data", data);
        Set<String> keys = 
        		accountMap.keySet()
        		.stream()
        		.collect(Collectors.toCollection(TreeSet::new));
        
        keys.stream().forEach(key -> {
        	Tuple2<AccountBaseline, List<AccountTransaction>> value = accountMap.get(key);
        	AccountBaseline accountBaseline = value.getKey();
        	
        	List<String> datum = new ArrayList<>();
        	datum.add(key);
        	datum.add("" + accountBaseline.getOwner());
        	datum.add("" + accountBaseline.getWhen());
        	datum.add("" + this.currencySymbol + TWO_DP.format(accountBaseline.getBalance()));
        	
        	data.add(datum);
        });
        
        return modelAndView;
    }

	/**
	 * <p>Detailed account info for one account, transactions
	 * </p>
	 *
	 * @param httpSession
	 * @return
	 */
    @GetMapping("/index2")
    public ModelAndView index2(HttpServletRequest httpServletRequest, HttpSession httpSession) {
    	String j_account = httpServletRequest.getParameter("j_account");
    	if (j_account==null) {
    		j_account="";
    	}
        log.info("index2({}), session={}", j_account, httpSession.getId());

        IMap<String, Tuple2<AccountBaseline, List<AccountTransaction>>> accountMap = 
        		this.hazelcastInstance.getMap(MyConstants.IMAP_NAME_ACCOUNT);

        ModelAndView modelAndView = new ModelAndView("account/index2");

    	Tuple2<AccountBaseline, List<AccountTransaction>> value = accountMap.get(j_account);
    	
    	if (value != null) {
        	AccountBaseline accountBaseline = value.getKey();
        	
        	modelAndView.addObject("account", 
        		j_account);
        	modelAndView.addObject("holder", 
            	accountBaseline.getOwner()+"");
        	modelAndView.addObject("balance", 
        		this.currencySymbol + TWO_DP.format(accountBaseline.getBalance()));

            List<String> columns = new ArrayList<>();
            columns.add("Date");
            columns.add("Description");
            columns.add("Debit");
            columns.add("Credit");
            modelAndView.addObject("columns", columns);

            List<List<String>> data = new ArrayList<>();
            modelAndView.addObject("data", data);
            
            for (AccountTransaction accountTransaction : value.getValue()) {
            	List<String> datum = new ArrayList<>();

            	datum.add("" + accountTransaction.getWhen());
            	datum.add("" + accountTransaction.getDescription());
            	
            	// Debit amount and blank credit, or vice versa
            	if (accountTransaction.getDebit()) {
            		datum.add("" + TWO_DP.format(accountTransaction.getAmount()));
            		datum.add("");
            	} else {
            		datum.add("");
            		datum.add("" + TWO_DP.format(accountTransaction.getAmount()));
            	}
            	
            	data.add(datum);
            }
    	}
        
        return modelAndView;
    }

}
