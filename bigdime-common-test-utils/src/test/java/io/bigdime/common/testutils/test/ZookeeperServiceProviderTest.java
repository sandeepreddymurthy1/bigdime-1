/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;

import io.bigdime.common.testutils.provider.ZookeeperServerProvider;

import java.io.IOException;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ZookeeperServiceProviderTest {
	ZookeeperServerProvider zkServiceProvider = null;
	@BeforeTest
	public void startup(){
		zkServiceProvider =new ZookeeperServerProvider();

	}
	@Test
	public void testgetZookeeperSevrer() throws IOException {
		ZooKeeperServer zs = 	zkServiceProvider.getZookeeperSevrer(null, null, 0);
		Assert.assertNotNull(zs);
	}
	
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testgetServerCnxnFactory() throws IOException{
		ServerCnxnFactory scFactory = zkServiceProvider.getServerCnxnFactory(null, 0, 0);	
		Assert.assertNull(scFactory);
	}
}
