/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import org.testng.Assert;
import org.testng.annotations.Test;

public class AdaptorConfigTest {

	/**
	 * 
	 */
	@Test
	public void testGetName() {
		String name = AdaptorConfig.getInstance().getName();
		AdaptorConfig.getInstance().setName(null);
		Assert.assertNotNull(AdaptorConfig.getInstance().getName(), "Either a valid name of NO-NAME must be returned");
		AdaptorConfig.getInstance().setName(name);
	}
}
