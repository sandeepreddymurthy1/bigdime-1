/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@PrepareForTest({ ManagementController.class })
public class ManagementControllerTest extends PowerMockTestCase{
	private static final Logger logger = LoggerFactory
			.getLogger(ManagementControllerTest.class);
	ManagementController managementController;

	@BeforeClass
	public void init() {
		managementController = new ManagementController();
	}

	@BeforeTest
	public void setup() {
		logger.info("source type", "Test Phase", "Setting the environment");
		System.setProperty("env", "test");
	}

	@Test
	public void settersAndGettersTest() {
		AuthenticationService authenticationService = new AuthenticationService();
		managementController.setAuthenticationService(authenticationService);
		Assert.assertSame(managementController.getAuthenticationService(),
				authenticationService);

	}

	@Test
	public void authenticateUserTest() {
		ServletRequest request = Mockito.mock(ServletRequest.class);
		Assert.assertEquals("index",
				managementController.authenticateUser(request).getViewName());
	}

	@Test
	public void homeTest() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
		HttpSession session = Mockito.mock(HttpSession.class);
		Mockito.when(request.getSession(Mockito.anyBoolean())).thenReturn(
				session);
		Mockito.when(request.getParameter(Mockito.anyString())).thenReturn(
				"test");
		AuthenticationService authenticationService = Mockito
				.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser = Mockito
				.mock(AuthenticateUser.class);
		PowerMockito.whenNew(AuthenticateUser.class).withNoArguments().thenReturn(authenticateUser);
		ReflectionTestUtils.setField(managementController, "ldapEnabled", true);
		ReflectionTestUtils.setField(managementController,
				"authenticationService", authenticationService);
		Assert.assertEquals("index",
				managementController.home(request, response));
	}

	@Test
	public void homeldapNotEnabledTest() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
		HttpSession session = Mockito.mock(HttpSession.class);
		Mockito.when(request.getSession(Mockito.anyBoolean())).thenReturn(
				session);
		Mockito.when(request.getParameter(Mockito.anyString())).thenReturn(
				"test");
		AuthenticationService authenticationService = Mockito
				.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser = Mockito
				.mock(AuthenticateUser.class);
		PowerMockito.whenNew(AuthenticateUser.class).withNoArguments().thenReturn(authenticateUser);	
		ReflectionTestUtils
				.setField(managementController, "ldapEnabled", false);
		ReflectionTestUtils.setField(managementController,
				"authenticationService", authenticationService);
		Assert.assertEquals("index",
				managementController.home(request, response));
	}

	@Test
	public void homeLoginStatusTrueTest() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
		HttpSession session = Mockito.mock(HttpSession.class);
		Mockito.when(request.getSession(Mockito.anyBoolean())).thenReturn(
				session);
		Mockito.when(request.getParameter(Mockito.anyString())).thenReturn(
				"test");
		AuthenticationService authenticationService = Mockito
				.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser = Mockito
				.mock(AuthenticateUser.class);
		PowerMockito.whenNew(AuthenticateUser.class).withNoArguments().thenReturn(authenticateUser);
		Mockito.when(authenticateUser.getLoginStatus()).thenReturn(true);
		ReflectionTestUtils.setField(managementController, "ldapEnabled", true);
		ReflectionTestUtils.setField(managementController,
				"authenticationService", authenticationService);
		Assert.assertEquals("home",
				managementController.home(request, response));
	}

	@Test
	public void homeLoginStatusFalseTest() throws Exception {
		HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
		HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
		HttpSession session = Mockito.mock(HttpSession.class);
		Mockito.when(request.getSession(Mockito.anyBoolean())).thenReturn(
				session);
		Mockito.when(request.getParameter(Mockito.anyString())).thenReturn(
				"test");
		AuthenticationService authenticationService = Mockito
				.mock(AuthenticationService.class);
		AuthenticateUser authenticateUser = Mockito
				.mock(AuthenticateUser.class);
		PowerMockito.whenNew(AuthenticateUser.class).withNoArguments().thenReturn(authenticateUser);
		Mockito.when(authenticateUser.getLoginStatus()).thenReturn(false);
		ReflectionTestUtils.setField(managementController, "ldapEnabled", true);
		ReflectionTestUtils.setField(managementController,
				"authenticationService", authenticationService);
		Assert.assertEquals("index",
				managementController.home(request, response));

	}

}
