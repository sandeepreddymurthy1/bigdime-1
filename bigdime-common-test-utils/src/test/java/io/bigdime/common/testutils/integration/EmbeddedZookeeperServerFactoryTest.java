/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.integration;

import static io.bigdime.common.testutils.test.TestUtilsConstants.DEV;
import static io.bigdime.common.testutils.test.TestUtilsConstants.ENV;
import io.bigdime.common.testutils.TestUtils;
import io.bigdime.common.testutils.factory.EmbeddedZookeeperServerFactory;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author jbrinnand
 */
public class EmbeddedZookeeperServerFactoryTest {
	private Logger logger = LoggerFactory.getLogger(EmbeddedZookeeperServerFactoryTest.class);
	private EmbeddedZookeeperServerFactory zksUtil;
	private static final String USER_DIR = "user.dir";
	private static final String DATA_DIR = "/src/test/resources/zkdata";
	private static final String DATA_LOG_DIR = "/src/test/resources/zkdata-log";
	private String dataDir = null;
	private String dataLogDir = null;

	@BeforeTest
	public void setup() {
		logger.info("Setting the environment");
		System.setProperty(ENV, DEV);
		System.setProperty("ZK_ENABLED", Boolean.TRUE.toString());
	}

	@BeforeClass
	public void init() throws Exception {
		// Zookeeper must be started prior
		// to the web application, so that the
		// zookeeper client in the web application
		// can communicate with the zookeeper server.
		// ************************************
		dataDir = System.getProperty(USER_DIR) + DATA_DIR;
		dataLogDir = System.getProperty(USER_DIR) + DATA_LOG_DIR;
		zksUtil = EmbeddedZookeeperServerFactory.getInstance(dataDir, dataLogDir);
	}

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(EmbeddedZookeeperServerFactory.getInstance(dataDir, dataLogDir, "", 0));
	}

	//@Test
	public void testEmbededZookeeper() throws InterruptedException, IOException {
		zksUtil.startZookeeper();
		logger.info("Zookeeper status: " + zksUtil.getZookeeperServer().isRunning());
		Assert.assertEquals(true, zksUtil.isRunning());
		Thread.sleep(3000);

		zksUtil.shutdownZookeeper();
		Assert.assertNotEquals(true, zksUtil.getZookeeperServer().isRunning());
	}
}
