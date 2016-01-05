/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.provider;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
/**
 * 
 * @author mnamburi
 *
 */
public class ZookeeperServerProvider {
	/**
	 * 
	 * @param dataDir
	 * @param dataLogicDir
	 * @param tickTime
	 * @return
	 * @throws IOException
	 */
	public ZooKeeperServer getZookeeperSevrer(File dataDir,File dataLogicDir,int tickTime) throws IOException{
		ZooKeeperServer zks = new ZooKeeperServer(dataDir, dataLogicDir, tickTime);
		return zks;
	}
	/**
	 * 
	 * @param host
	 * @param port
	 * @param time
	 * @return
	 * @throws IOException
	 */
	public ServerCnxnFactory getServerCnxnFactory(String host, int port,int maxClientCnxns) throws IOException{
		ServerCnxnFactory serverCnxFactory = NettyServerCnxnFactory.createFactory(new InetSocketAddress(
				host, port), maxClientCnxns);
		return serverCnxFactory;
	}
}
