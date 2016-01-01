/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client.exception;


import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.exception.HBaseClientException;
import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
public class HBaseClientExceptionTest {
	private static final Logger logger = LoggerFactory.getLogger(HBaseClientExceptionTest.class);

	HBaseClientException hBaseClientException;
   @Test
   public void HBaseClientExceptionStringConstructerTest(){
	   hBaseClientException=new HBaseClientException(TEST);
	   Assert.assertEquals(hBaseClientException.getMessage(), TEST);	   
   }
   
   @Test
   public void HBaseClientExceptionThrowableConstructerTest(){
	   hBaseClientException=new HBaseClientException(TEST,new Exception(TEST));
	   Assert.assertEquals(hBaseClientException.getMessage(), TEST);
	   Assert.assertEquals(hBaseClientException.getCause().getMessage(), TEST);
   }
}
