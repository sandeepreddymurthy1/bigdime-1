/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.constants;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.handler.constants.WebHDFSWriterHandlerConstants;

public class WebHDFSWriterHandlerConstantsTest {

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(WebHDFSWriterHandlerConstants.getInstance());
	}
}
