/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.splunkimpl.splunkalert.test;

import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.TEST_EXCEPTION;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIORNMENT;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.ENVIRONMENT_VALUE;
import static io.bigdime.splunkimpl.splunkalert.constants.TestResourceConstants.SOURCE_TYPE;
import io.bigdime.splunkalert.common.exception.AuthorizationException;

import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
/**
 * 
 * @author Sandeep Reddy,Murthy
 *
 */
public class AuthorizationExceptionTest extends PowerMockTestCase {
	private static final Logger logger = LoggerFactory.getLogger(AuthorizationExceptionTest.class);
	
	@BeforeTest
	public void setup() {
		logger.info(SOURCE_TYPE,"Test Phase","Setting the environment");
		System.setProperty(ENVIORNMENT, ENVIRONMENT_VALUE);
		
	}
	
  @Test
  public void authorizationExceptionTests(){      
	 Assert.assertEquals(TEST_EXCEPTION,new AuthorizationException(TEST_EXCEPTION).getMessage());
	 Assert.assertEquals(TEST_EXCEPTION,new AuthorizationException("", new Throwable(TEST_EXCEPTION)).getCause().getMessage());
	 
  }
  

}
