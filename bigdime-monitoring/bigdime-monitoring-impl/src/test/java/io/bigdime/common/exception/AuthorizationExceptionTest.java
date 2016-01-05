/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.exception;

import static io.bigdime.constants.TestResourceConstants.TEST_STRING;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AuthorizationExceptionTest {

private static final Logger logger = LoggerFactory.getLogger(AuthorizationExceptionTest.class);
	
	@Test
	public void authorizationExceptionDefaultConstructerTest(){
		AuthorizationException authorizationException=new AuthorizationException(TEST_STRING);
		Assert.assertEquals(authorizationException.getMessage(),TEST_STRING);
	}
	
	@Test
	public void authorizationExceptionOverloasdedConstructerTest(){
		AuthorizationException authorizationException=new AuthorizationException(TEST_STRING, new Exception(TEST_STRING));
		Assert.assertEquals(authorizationException.getMessage(),TEST_STRING);
		Assert.assertEquals(authorizationException.getCause().getMessage(), TEST_STRING);
	}
	
	
}
