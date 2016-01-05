/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.client;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKClientBaseTest {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperDiscoveryServiceTests.class);

	public static final String DEFAULT_HOST = "127.0.0.1";
	public static final int DEFAULT_PORT = 4183;
	public static final String DEFAULT_HOST_PORT = "127.0.0.1:4183";

	static{
		TestingServer testZookeeperServer;
		try {
			testZookeeperServer = new TestingServer(DEFAULT_PORT);
			testZookeeperServer.start();	
		} catch (Exception e) {
			logger.error("error= {}",e);
		}	
	}
}
