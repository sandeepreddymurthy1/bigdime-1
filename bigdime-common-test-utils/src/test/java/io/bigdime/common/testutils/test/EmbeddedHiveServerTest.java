/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;

import static io.bigdime.common.testutils.test.TestUtilsConstants.DEV;
import static io.bigdime.common.testutils.test.TestUtilsConstants.ENV;

import java.io.IOException;

import io.bigdime.common.testutils.factory.EmbeddedHiveServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class EmbeddedHiveServerTest {
	private Logger logger = LoggerFactory.getLogger(EmbeddedHiveServerTest.class); 
	EmbeddedHiveServer embeddedHiveServer = null;

	@BeforeTest
	public void beforeClass() {
		logger.info("Setting the environment");
		System.setProperty(ENV, DEV);
		embeddedHiveServer = EmbeddedHiveServer.getInstance(); 
	}

	@Test
	public void testEmbededHive() throws InterruptedException, IOException {
		Assert.assertFalse(embeddedHiveServer.serverStarted());
		embeddedHiveServer.startMetaStore();
		Assert.assertTrue(embeddedHiveServer.serverStarted());
	}
}
