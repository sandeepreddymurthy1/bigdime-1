/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import org.testng.annotations.Test;

public class AdaptorConfigConstantsTest {

	@Test
	public void testSourceConfigConstants() {
		new AdaptorConfigConstants();
		new AdaptorConfigConstants.SourceConfigConstants();
		new AdaptorConfigConstants.ChannelConfigConstants();
		new AdaptorConfigConstants.SinkConfigConstants();
		new AdaptorConfigConstants.HandlerConfigConstants();
	}
}
