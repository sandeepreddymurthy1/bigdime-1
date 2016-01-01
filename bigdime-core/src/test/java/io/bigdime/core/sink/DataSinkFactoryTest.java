/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Random;
import java.util.Set;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Sink;
import io.bigdime.core.commons.StringHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.HandlerConfig;
import io.bigdime.core.config.SinkConfig;
import io.bigdime.core.handler.DummyConcreteHandler;
import io.bigdime.core.handler.HandlerFactory;
import io.bigdime.core.handler.HandlerFactoryTest;

@Configuration
@ContextConfiguration(classes = { DataSinkFactory.class, HandlerFactory.class, DummyConcreteHandler.class,
		StringHelper.class })
public class DataSinkFactoryTest extends AbstractTestNGSpringContextTests {
	@Autowired
	DataSinkFactory dataSinkFactory;

	@Test
	public void testGetInstance() {
		Assert.assertNotNull(dataSinkFactory);
	}

	public DataSinkFactoryTest() throws IOException {
		HandlerFactoryTest.initHandlerFactory();
	}

	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testGetDataSinkWithNullConfig() throws AdaptorConfigurationException {
		dataSinkFactory.getDataSink(null);
	}

	/**
	 * Test that correct number of Sink objects are created for given
	 * SinkConfig. This test adds more than one channel-descs and expects same
	 * number of Sink objects created.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetDataSinkForTopicParitionInputs() throws AdaptorConfigurationException {
		SinkConfig sinkConfig = new SinkConfig();
		int channelDescSize = 0;// Set the number of channels to be >1
		Random rand = new Random();
		while (channelDescSize < 2) {
			channelDescSize = rand.nextInt(10);
		}
		Set<String> channelDescs = new HashSet<>();
		for (int i = 0; i < channelDescSize; i++) {
			channelDescs.add("unit-channel-" + i);
		}

		sinkConfig.setChannelDescs(channelDescs);

		HandlerConfig handlerConfig1 = Mockito.mock(HandlerConfig.class);
		LinkedHashSet<HandlerConfig> handlerConfigs = new LinkedHashSet<>();
		Mockito.when(handlerConfig1.getName()).thenReturn("unit-handler-name-1");
		Mockito.when(handlerConfig1.getHandlerClass()).thenReturn("io.bigdime.core.handler.DummyConcreteHandler");

		handlerConfigs.add(handlerConfig1);
		sinkConfig.setHandlerConfigs(handlerConfigs);

		final Set<DataChannel> channels = new HashSet<>();
		DataChannel[] mockChannels = new DataChannel[channelDescSize];
		for (int i = 0; i < channelDescSize; i++) {
			mockChannels[i] = Mockito.mock(DataChannel.class);
			Mockito.when(mockChannels[i].getName()).thenReturn("unit-channel-" + i);
			channels.add(mockChannels[i]);
		}

		AdaptorConfig.getInstance().getAdaptorContext().setChannels(channels);
		Collection<Sink> sinks = dataSinkFactory.getDataSink(sinkConfig);
		Assert.assertEquals(sinks.size(), channelDescSize);
		for (int i = 0; i < channelDescSize; i++) {
			Mockito.verify(mockChannels[i], Mockito.times(1)).registerConsumer("unit-handler-name-1");
		}
	}
}
