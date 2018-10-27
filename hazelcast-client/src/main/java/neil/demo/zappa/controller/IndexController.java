package neil.demo.zappa.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import lombok.extern.slf4j.Slf4j;

/**
 * <p>A controller for the front page, "{@code index.html}" that
 * drives the main menu.
 * </p>
 */
@Controller
@Slf4j
public class IndexController {

    @GetMapping("/")
    public String index(HttpSession httpSession, HttpServletRequest httpServletRequest) {
        log.info("index(), session={}", httpSession.getId());

        // Capture the user agent (ie. Chrome, Safari, Firefox)
        if (httpSession.getAttribute(HttpHeaders.USER_AGENT) == null) {
            String userAgent = httpServletRequest.getHeader(HttpHeaders.USER_AGENT);
            httpSession.setAttribute(HttpHeaders.USER_AGENT, (userAgent == null ? "null" : userAgent));
        }
        
        return "index";
    }

}
