/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;


import java.util.Date;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.bigdime.constants.TestResourceConstants.TEST_STRING;

/**
 * 
 * @author samurthy
 *
 */

public class AlertTest {

	private static final Logger logger = LoggerFactory.getLogger(AlertTest.class);
	
	@Test
	public void validateAlertTestGettersandSetters() {
		
		Alert alert =new Alert();
		alert.setApplicationName(TEST_STRING);
		alert.setCause(ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
		alert.setMessage(TEST_STRING);
        alert.setMessageContext(TEST_STRING);
        alert.setSeverity(ALERT_SEVERITY.NORMAL);
        alert.setType(ALERT_TYPE.ADAPTOR_FAILED_TO_START);
       
        Assert.assertEquals(alert.getApplicationName(), TEST_STRING);
		Assert.assertEquals(alert.getCause(), ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
	    Assert.assertEquals(alert.getMessage(), TEST_STRING);
	    Assert.assertEquals(alert.getMessageContext(), TEST_STRING);
	    Assert.assertEquals(alert.getSeverity(), ALERT_SEVERITY.NORMAL);
	    Assert.assertEquals(alert.getType(), ALERT_TYPE.ADAPTOR_FAILED_TO_START);
	    
	}
		
}
