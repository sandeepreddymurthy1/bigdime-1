/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryChannelInputHandlerConstantsTest {

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(MemoryChannelInputHandlerConstants.getInstance());
	}
}
