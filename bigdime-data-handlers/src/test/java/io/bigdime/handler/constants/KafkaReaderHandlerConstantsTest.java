/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.constants;

import org.testng.Assert;
import org.testng.annotations.Test;

public class KafkaReaderHandlerConstantsTest {
	@Test
	public void testGetInstance() {
		Assert.assertNotNull(KafkaReaderHandlerConstants.getInstance());
	}

}
