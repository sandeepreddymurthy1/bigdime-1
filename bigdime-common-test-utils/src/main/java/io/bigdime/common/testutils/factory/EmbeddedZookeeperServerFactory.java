/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.factory;


import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.bigdime.common.testutils.provider.ZookeeperServerProvider;

/**
 * @author jbrinnand
 */
public class EmbeddedZookeeperServerFactory {
	private Logger logger = LoggerFactory.getLogger(EmbeddedZookeeperServerFactory.class); 
	private File dataDir;
	private File dataLogicDir;
	private static int DEFAULT_PORT = 2182;
	private static String DEFAULT_HOST = "127.0.0.1";
	private static final int DEFAULT_TICK_TIME = 50000;
	private ZooKeeperServer zks = null;
	private ServerCnxnFactory factory;
	private ZookeeperServerProvider provider = null;
	private final long sleepTime = 1000l;
	private final int maxClientCnxns = 10;
	private final int sessionTimeOut = 240000;

	protected EmbeddedZookeeperServerFactory (String dataDir, String dataLogicDir,String host,int port) {
		provider = new ZookeeperServerProvider();
		this.dataDir = new File(dataDir);
		this.dataLogicDir = new File (dataLogicDir);
		if(host != null)
			DEFAULT_HOST = host;
		if(port != 0)
			DEFAULT_PORT = port;		
	}

	public static EmbeddedZookeeperServerFactory getInstance(String dataDir, String dataLogicDir,String host,int port){
			return new EmbeddedZookeeperServerFactory(dataLogicDir, dataLogicDir,host,port);

	}
	public static EmbeddedZookeeperServerFactory getInstance (String dataDir, String dataLogicDir) {
			return  getInstance(dataLogicDir, dataLogicDir,DEFAULT_HOST,DEFAULT_PORT);
	}
	public void startZookeeper() throws IOException, InterruptedException {
		zks = provider.getZookeeperSevrer(dataDir, dataLogicDir, DEFAULT_TICK_TIME);
		zks.setMinSessionTimeout(sessionTimeOut);
		InetAddress.getLocalHost();
		factory = provider.getServerCnxnFactory(DEFAULT_HOST, DEFAULT_PORT, maxClientCnxns);	
		factory.startup(zks);
		while (!zks.isRunning()) {
			Thread.sleep(sleepTime);
		}
	}
	public ZooKeeperServer getZookeeperServer () {
		return zks;
	}
	public boolean isRunning() {
		return zks.isRunning();
	}

	public void shutdownZookeeper() throws IOException {
		// Make sure to clean up the data directories
		// or zookeeper may not elect a leader
		// the next time around.
		//**********************************
		factory.shutdown();
		zks.shutdown();
		FileUtils.deleteDirectory(dataDir);
		FileUtils.deleteDirectory(dataLogicDir);		
		logger.info("Zookeeper running - status is: " + isRunning());
	}
}
