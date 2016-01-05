/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_TYPE;

public class LoggerTest {

	@Test
	public void testAlertType() {
		ALERT_TYPE type = ALERT_TYPE.ADAPTOR_FAILED_TO_START;
		Assert.assertEquals(type.getDescription(), "adaptor failed to start");
		Assert.assertEquals(type.getMessageCode(), "BIG-0001");
	}

	@Test
	public void testAlertCause() {
		ALERT_CAUSE cause = ALERT_CAUSE.APPLICATION_INTERNAL_ERROR;
		Assert.assertEquals(cause.getDescription(), "internal error");
	}
}
