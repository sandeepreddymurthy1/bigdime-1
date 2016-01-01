/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;


import io.bigdime.common.testutils.factory.EmbeddedZookeeperServerFactory;
import io.bigdime.common.testutils.provider.ZookeeperServerProvider;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * @author mnamburi
 */
public class EmbeddedZookeeperServerFactoryTest {
	private static final String DATA_DIR = "/src/test/resources/zkdata";
	private static final String DATA_LOG_DIR = "/src/test/resources/zkdata-log";
	
	@Test
	public void testZookeeperFactoryStart() throws IOException, InterruptedException{
		File file = mock(File.class);
		ZookeeperServerProvider provider = mock(ZookeeperServerProvider.class);
		ZooKeeperServer zookeeperServer = mock(ZooKeeperServer.class);
		ServerCnxnFactory factory =  mock(ServerCnxnFactory.class);
		EmbeddedZookeeperServerFactory embeddedZK = EmbeddedZookeeperServerFactory.getInstance(DATA_DIR, DATA_LOG_DIR);
		ReflectionTestUtils.setField(embeddedZK, "dataDir", file);
		ReflectionTestUtils.setField(embeddedZK, "dataLogicDir", file);
		ReflectionTestUtils.setField(embeddedZK, "zks", zookeeperServer);
		ReflectionTestUtils.setField(embeddedZK, "factory", factory);
		ReflectionTestUtils.setField(embeddedZK, "provider", provider);
		when(provider.getZookeeperSevrer(any(File.class), any(File.class), anyInt())).thenReturn(zookeeperServer);
		when(provider.getServerCnxnFactory(anyString(), anyInt(), anyInt())).thenReturn(factory);
		when(zookeeperServer.isRunning()).thenReturn(true);
		embeddedZK.startZookeeper();
		Assert.assertNotNull(embeddedZK.getZookeeperServer());
	}
	
	@Test
	public void testZookeeperFactoryStop() throws IOException{
		File file = mock(File.class);
		ZookeeperServerProvider provider = mock(ZookeeperServerProvider.class);
		ZooKeeperServer zookeeperServer = mock(ZooKeeperServer.class);
		ServerCnxnFactory factory =  mock(ServerCnxnFactory.class);
		EmbeddedZookeeperServerFactory embeddedZK = EmbeddedZookeeperServerFactory.getInstance(DATA_DIR, DATA_LOG_DIR, "", 0);
		ReflectionTestUtils.setField(embeddedZK, "dataDir", file);
		ReflectionTestUtils.setField(embeddedZK, "dataLogicDir", file);
		ReflectionTestUtils.setField(embeddedZK, "zks", zookeeperServer);
		ReflectionTestUtils.setField(embeddedZK, "factory", factory);
		ReflectionTestUtils.setField(embeddedZK, "provider", provider);
		embeddedZK.shutdownZookeeper();		
	}
}
