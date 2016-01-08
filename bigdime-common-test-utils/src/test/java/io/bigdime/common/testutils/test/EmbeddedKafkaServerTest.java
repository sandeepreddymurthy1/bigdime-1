/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils.test;

import java.util.concurrent.TimeUnit;

import kafka.controller.KafkaController;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import io.bigdime.common.testutils.factory.EmbeddedKafkaServer;
import static org.mockito.Mockito.*;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
/**
 * 
 * @author mnamburi
 *
 */
public class EmbeddedKafkaServerTest {
	EmbeddedKafkaServer embeddedKafkaServer = null;
	@BeforeTest
	public void beforeTest() throws Exception{
		StringBuilder seedBrokers = new StringBuilder();
		seedBrokers.append("kafka.provider.one:"+9092);
		seedBrokers.append("kafka.provider.two:"+9093);
		
		embeddedKafkaServer = EmbeddedKafkaServer.newServer()
				.host("kafka.provider.one")
				.brokers(seedBrokers.toString().split(","))
				.port("1212")
				.brokerId("0")
				.zookeeperConnection("kafka.provider.one", 2181, "kafka")
				.logDirs(""+"/kafkaLogs1")
				.numPartitions("2")
				.logRetention(TimeUnit.MINUTES, 10)
				.messageMaxBytes("1000000")
				.defaultReplicationFactor("2")
				.replicaFetchMaxBytes("1000000")
				.build();
	}
	@Test
	public void testEmbeddedKafkaServer() throws Exception{

		KafkaServer kafkaServer = mock(KafkaServer.class);
		SocketServer socketServer = mock(SocketServer.class);
		KafkaController kafkaController = mock(KafkaController.class);
		KafkaConfig kafkaConfig = mock(KafkaConfig.class);

		
		ReflectionTestUtils.setField(embeddedKafkaServer, "kafkaServer", kafkaServer);

		embeddedKafkaServer.startup();
		when(kafkaServer.kafkaController()).thenReturn(kafkaController);
		when(kafkaServer.socketServer()).thenReturn(socketServer);
		when(kafkaServer.config()).thenReturn(kafkaConfig);
		when(kafkaController.isActive()).thenReturn(Boolean.FALSE).thenReturn(Boolean.TRUE).thenReturn(Boolean.FALSE);
		Assert.assertNotNull(embeddedKafkaServer.getServer());
		Assert.assertFalse(embeddedKafkaServer.isActive());
		embeddedKafkaServer.shutdown();
	}
}
