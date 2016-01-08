/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.constants.TestResourceConstants.TEST_INT;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AlertServiceResponseTest {

	private static final Logger logger = LoggerFactory
			.getLogger(AlertServiceResponseTest.class);

	@Test
	public void alertServiceResponseGettessAndSettersTest() {

		AlertServiceResponse<Alert> alertServiceResponse = new AlertServiceResponse<Alert>();
		Alert alert = new Alert();
		List<Alert> list = new ArrayList<Alert>();
		list.add(alert);
		list.add(alert);
		alertServiceResponse.setAlerts(list);
		alertServiceResponse.setNumFound(TEST_INT);

		Assert.assertEquals(alertServiceResponse.getNumFound(), TEST_INT);
		Assert.assertTrue(alertServiceResponse.getAlerts().get(TEST_INT) instanceof Alert);

	}
}
