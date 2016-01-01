/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.integration;

import static io.bigdime.common.testutils.test.TestUtilsConstants.*;

import java.io.IOException;

import io.bigdime.common.testutils.TestUtils;
import io.bigdime.common.testutils.factory.EmbeddedTomcatFactory;

import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * 
 * @author mnamburi
 *
 */
public class EmbeddedTomcatFactoryTest  {
	private Logger logger = LoggerFactory.getLogger(EmbeddedTomcatFactoryTest.class); 
	private static EmbeddedTomcatFactory tomcatFactory =  EmbeddedTomcatFactory.getInstance();

	@BeforeTest
	public void beforeClass() {
		logger.info("Setting the environment");
		System.setProperty(ENV, DEV);
	}

	@Test
	public void testEmbededTomcat() throws ServletException, 
	LifecycleException, InterruptedException, IOException {
		tomcatFactory.startTomcat(TestUtils.findAvailablePort(zeroPort),"Test-App");
		Assert.assertTrue(tomcatFactory.isRunning());
		Assert.assertNotNull(tomcatFactory.getContext());
		tomcatFactory.shutdownTomcat();
		Assert.assertFalse(tomcatFactory.isRunning());
	}
}
