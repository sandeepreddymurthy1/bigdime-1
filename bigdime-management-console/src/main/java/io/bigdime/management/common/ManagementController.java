/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import static io.bigdime.management.common.ApplicationConstants.ROOT;
import static io.bigdime.management.common.ApplicationConstants.INDEXROOT;
import static io.bigdime.management.common.ApplicationConstants.INDEX;
import static io.bigdime.management.common.ApplicationConstants.COMMAND;
import static io.bigdime.management.common.ApplicationConstants.HOMEROOT;
import static io.bigdime.management.common.ApplicationConstants.USERNAME;
import static io.bigdime.management.common.ApplicationConstants.PASSWORD;
import static io.bigdime.management.common.ApplicationConstants.TESTUSER;
import static io.bigdime.management.common.ApplicationConstants.LOGINRESULT;
import static io.bigdime.management.common.ApplicationConstants.SUCCESS;
import static io.bigdime.management.common.ApplicationConstants.DISPLAY_NAME;
import static io.bigdime.management.common.ApplicationConstants.USER_ID;
import static io.bigdime.management.common.ApplicationConstants.HOME;
import static io.bigdime.management.common.ApplicationConstants.ERROR;



@Controller
@RequestMapping
public class ManagementController {

	@Value("${ldap.enabled}")
	private boolean ldapEnabled;
	@Autowired
	private AuthenticationService authenticationService;

	public AuthenticationService getAuthenticationService() {
		return authenticationService;
	}

	public void setAuthenticationService(
			AuthenticationService authenticationService) {
		this.authenticationService = authenticationService;
	}

	@RequestMapping(method = RequestMethod.GET, value = { ROOT, INDEXROOT })
	public ModelAndView authenticateUser(ServletRequest request) {
		return new ModelAndView(INDEX, COMMAND, new AuthenticateUser());
	}

	@RequestMapping(method = RequestMethod.POST, value = HOMEROOT)
	public String home(HttpServletRequest request, HttpServletResponse response) {
		HttpSession session = request.getSession(true);
		String userName = request.getParameter(USERNAME);
		String password = request.getParameter(PASSWORD);
		AuthenticateUser authenticateUser = new AuthenticateUser();
		authenticateUser.setUserId(userName);
		authenticateUser.setPassword(password);
		if (ldapEnabled) {
			authenticationService.authenticate(authenticateUser);
		} else {
			authenticateUser.setDisplayName(TESTUSER);
			authenticateUser.setLoginStatus(true);
		}

		if (authenticateUser.getLoginStatus() == true) {
			session.setAttribute(LOGINRESULT, SUCCESS);
			session.setAttribute(DISPLAY_NAME,
					authenticateUser.getDisplayName());
			session.setAttribute(USER_ID, authenticateUser.getUserId());
			return (HOME);
		} else {
			session.setAttribute(LOGINRESULT, ERROR);
			return (INDEX);
		}

	}

}
