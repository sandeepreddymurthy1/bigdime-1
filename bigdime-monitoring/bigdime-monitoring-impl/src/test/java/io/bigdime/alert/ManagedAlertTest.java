/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.constants.TestResourceConstants.TEST_STRING;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ManagedAlertTest {
	
private static final Logger logger = LoggerFactory.getLogger(ManagedAlertTest.class);
	
	@Test
	public void managedAlertGettersAndSettersTest(){
		ManagedAlert managedAlert=new ManagedAlert();
		managedAlert.setComment(TEST_STRING);
		managedAlert.setAlertStatus(ALERT_STATUS.ACKNOWLEDGED);
		
		Assert.assertEquals(managedAlert.getComment(), TEST_STRING);
		Assert.assertEquals(managedAlert.getAlertStatus(), ALERT_STATUS.ACKNOWLEDGED);
		
	}

}
