/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.management.common;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AuthenticateUserTest {
	private static final Logger logger = LoggerFactory
			.getLogger(AuthenticateUserTest.class);
	AuthenticateUser authenticateUser;
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void authenticateUsersGettersAndSettersTest(){
		authenticateUser=new AuthenticateUser();
		authenticateUser.setDisplayName("testdisplayName");
		authenticateUser.setUserId("testUser");
		authenticateUser.setLoginStatus(true);
		authenticateUser.setPassword("testPassord");
		authenticateUser.setErrorCode("testError");
		Assert.assertEquals(authenticateUser.getDisplayName(), "testdisplayName");
		Assert.assertEquals(authenticateUser.getUserId(), "testUser");
		Assert.assertEquals(authenticateUser.getLoginStatus(), true);
		Assert.assertEquals(authenticateUser.getPassword(), "testPassord");
		Assert.assertEquals(authenticateUser.getErrorCode(), "testError");
		
	}
	
	@Test
	public void authenticateUserOverloadedConstructorTest(){
		authenticateUser=new AuthenticateUser("test");
		Assert.assertSame(authenticateUser.getUserId(), "test");
	}

}
