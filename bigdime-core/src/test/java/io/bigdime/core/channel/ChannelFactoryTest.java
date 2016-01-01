/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.HashMap;
import java.util.Map;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.InvalidDataTypeConfigurationException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.RequiredParameterMissingConfigurationException;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.config.AdaptorConfigConstants.ChannelConfigConstants;
import io.bigdime.core.config.ChannelConfig;

//TODO Find out the the best practice for using context configuration, individual classes or location for xml files.
@Configuration
@ContextConfiguration(classes = { JsonHelper.class, ChannelFactory.class, MemoryChannel.class })
public class ChannelFactoryTest extends AbstractTestNGSpringContextTests {
	@Autowired
	ChannelFactory channelFactory;

	/**
	 * Assert that if a valid channel-class is specified, getChannel method will
	 * return a non null value.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = NullPointerException.class)
	public void testGetChannelWithNullChannelConfig() throws AdaptorConfigurationException {
		Assert.assertNotNull(channelFactory.getChannel(getNullConfig()));
	}

	/*
	 * Have to use this method, Sonar doesnt like passing null directly to
	 * getChannel.
	 */
	private ChannelConfig getNullConfig() {
		return null;
	}

	/**
	 * Assert that if a valid channel-class is specified, getChannel method will
	 * return a non null value.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = RequiredParameterMissingConfigurationException.class)
	public void testGetChannelWithNullChannelClass() throws AdaptorConfigurationException {
		ChannelConfig channelConfig = new ChannelConfig();
		Assert.assertNotNull(channelFactory.getChannel(channelConfig));
	}

	/**
	 * Assert that if a valid channel-class is specified, getChannel method will
	 * return a non null value.
	 * 
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetChannel() throws AdaptorConfigurationException {
		ChannelConfig channelConfig = new ChannelConfig();
		channelConfig.setChannelProperties(new HashMap<String, Object>());
		channelConfig.setChannelClass("io.bigdime.core.channel.MemoryChannel");
		Assert.assertNotNull(channelFactory.getChannel(channelConfig));
	}

	/**
	 * Set an invalid class in the channel-class and make sure that channel
	 * can't be built.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = ClassNotFoundException.class)
	public void testNegativeWithInvalidChannelClass() throws Throwable {
		try {
			ChannelConfig channelConfig = new ChannelConfig();
			channelConfig.getChannelProperties().put(ChannelConfigConstants.CONCURRENCY, 1);
			channelConfig.setChannelClass("unit-channel-class-testNegativeWithInvalidChannelClass");
			channelFactory.getChannel(channelConfig);
			Assert.fail("should have thrown a AdaptorConfigurationException");
		} catch (AdaptorConfigurationException e) {
			throw e.getCause();
		}
	}

	/**
	 * If the concurrency value is set to 0 or less, throw
	 * InvalidValueConfigurationException.
	 */
	@Test(expectedExceptions = InvalidValueConfigurationException.class)
	public void testNegativeWithLessThanOneConcurrencyValue() throws AdaptorConfigurationException {
		ChannelConfig channelConfig = Mockito.mock(ChannelConfig.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> properties = Mockito.mock(Map.class);
		Mockito.when(channelConfig.getChannelProperties()).thenReturn(properties);
		Mockito.when(properties.get("concurrency")).thenReturn("0");
		Mockito.when(channelConfig.getChannelClass()).thenReturn("unit-channel-class");
		channelFactory.getChannel(channelConfig);
		Assert.fail("should have thrown a AdaptorConfigurationException");
	}

	/**
	 * If the concurrency value is set to a not-a-number value, throw
	 * InvalidDataTypeConfigurationException.
	 */
	@Test(expectedExceptions = InvalidDataTypeConfigurationException.class)
	public void testNegativeWithNotNumberConcurrencyValue() throws AdaptorConfigurationException {
		ChannelConfig channelConfig = Mockito.mock(ChannelConfig.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> properties = Mockito.mock(Map.class);
		Mockito.when(channelConfig.getChannelProperties()).thenReturn(properties);
		Mockito.when(properties.get("concurrency")).thenReturn("not-a-number");
		Mockito.when(channelConfig.getChannelClass()).thenReturn("unit-channel-class");
		channelFactory.getChannel(channelConfig);
		Assert.fail("should have thrown a AdaptorConfigurationException");
	}
}
