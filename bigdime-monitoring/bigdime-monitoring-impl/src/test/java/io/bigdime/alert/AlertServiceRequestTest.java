/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.constants.TestResourceConstants.TEST_STRING;
import static io.bigdime.constants.TestResourceConstants.TEST_INT;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertServiceRequestTest {
	
private static final Logger logger = LoggerFactory.getLogger(AlertServiceRequestTest.class);
	
	@Test
	public void alertServiceRequestGettersAndSettersTest(){
		
		AlertServiceRequest alertServiceRequest=new AlertServiceRequest();
		alertServiceRequest.setAlertId(TEST_STRING);
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		alertServiceRequest.setOffset(TEST_INT);
		alertServiceRequest.setLimit(TEST_INT);
		
		Assert.assertEquals(alertServiceRequest.getAlertId(), TEST_STRING);
		Assert.assertEquals(alertServiceRequest.getOffset(), TEST_INT);
		Assert.assertEquals(alertServiceRequest.getLimit(), TEST_INT);
	}

}
