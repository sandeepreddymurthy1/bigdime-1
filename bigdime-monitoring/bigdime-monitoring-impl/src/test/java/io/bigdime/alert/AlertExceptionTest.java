/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.constants.TestResourceConstants.TEST_STRING;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertExceptionTest {
	private static final Logger logger = LoggerFactory
			.getLogger(AlertExceptionTest.class);

	@Test
	public void alertExceptionDefaultConstructerTest() {
		AlertException alertException = new AlertException(TEST_STRING);
		Assert.assertEquals(alertException.getMessage(), TEST_STRING);
	}

	@Test
	public void alertExceptionOverloadedConstructerTest() {
		AlertException alertException = new AlertException(TEST_STRING,
				new Exception(TEST_STRING));
		Assert.assertEquals(alertException.getMessage(), TEST_STRING);
		Assert.assertEquals(alertException.getCause().getMessage(), TEST_STRING);
	}

}
