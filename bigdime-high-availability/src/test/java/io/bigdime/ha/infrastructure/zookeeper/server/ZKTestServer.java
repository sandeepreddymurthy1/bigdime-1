/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.ha.infrastructure.zookeeper.server;

import io.bigdime.ha.infrastructure.zookeeper.client.ZookeeperDiscoveryServiceTests;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * mnamburi
 * mock server to connect to Zookeeper
 */
public final class ZKTestServer {
	private static Logger logger = LoggerFactory.getLogger(ZookeeperDiscoveryServiceTests.class);

	public static final String DEFAULT_HOST = "127.0.0.1:4183";
	public static final int DEFAULT_PORT = 4183;
	public ZKTestServer() throws Exception{
		TestingServer testZookeeperServer = new TestingServer(DEFAULT_PORT);
		testZookeeperServer.start();
	}

}
